package org.jretty.kafka.xclient.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConsumerThread<K, V> extends Thread {

    protected static final Logger LOG = LoggerFactory.getLogger("FILTER-LOGGER");
    private final AtomicBoolean running = new AtomicBoolean(false);
    private long startTime;
    private final CountDownLatch processingCompleteLatch = new CountDownLatch(1);
    private final CountDownLatch waitConsumerCloseLatch = new CountDownLatch(1);

    private Consumer<K, V> consumer;
    private final List<String> topics;
    
    protected OffsetLogger offsetLogger = new OffsetLogger();
    
    private Map<String, Long> offsetMap;
    
    private long pollWaitTimeMs = 1000;
    
    // ~~ for pause control
    private final long sessionTimeoutMs;
    private boolean paused;
    private List<ConsumerRecords<K, V>> pausedRecords;
    private int pausePollDataSize;
    private int maxCacheRecordsWhenPause = 512000;
    
    private Future<?> future;
    private final ExecutorService executorService;
    // ~~end pause control 
    
    /**
     * @param threadName
     *            线程名称
     * @param topics
     *            需要消费的topic list（一个consumer消费一个APP的所有topics）
     * @param consumerProps
     *            consumer的配置
     * @param dataPushHandler
     *            处理数据推送的DataPushHandler实现类
     */
    public AbstractConsumerThread(final String threadName, List<String> topics, ConsumerFactory<K, V> consumerFactory) {
        super(threadName);
        this.topics = topics;
        this.consumer = consumerFactory.createConsumer();
        
        String sessionTimeoutStr = (String) consumerFactory.getConfigs().get("session.timeout.ms");
        this.sessionTimeoutMs = sessionTimeoutStr != null ? Long.valueOf(sessionTimeoutStr) - 10000 : 20000;
        LOG.info("use sessionTimeoutMs is " + sessionTimeoutMs);
        
        executorService = Executors.newFixedThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, threadName + "$handler$");
            }
        });
    }
    
    protected abstract void handleConsumerRecords(ConsumerRecords<K, V> records);
    
    private TopicPartition[] assignedPartitions;
    
    protected void pauseConsumer() {
        try {
            LOG.info("start to pauseConsumer topic-partition is {}", Arrays.toString(assignedPartitions));
            consumer.pause(assignedPartitions);
        } catch (Exception e) {
            LOG.warn("consumer.pause() fail due to ", e);
        }
    }
    
    protected void resumeConsumer() {
        try {
            LOG.info("resume paused Consumer topic-partition is {}", Arrays.toString(assignedPartitions));
            consumer.resume(assignedPartitions);
        } catch (Exception e) {
            LOG.warn("consumer.resume() fail due to ", e);
        }
    }
    
    /**
     * PushDataConsumerThread 开始工作，循环调用consumer.poll取数据并将数据丢给dataPushHandler处理
     */
    @Override
    public void run() {
        if(running.compareAndSet(false, true)) {
        try {
            ConsumerLog.info("'{}' start to run ... ...topics={}", this.getName(), topics.toString());
            
            consumer.subscribe(this.topics, new ConsumerRebalanceListener() {

                // 记录partition分配和回收状态

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    for (TopicPartition par : partitions) {
                        ConsumerLog.info("----------------PartitionsRevoked: " + par.toString());
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    AbstractConsumerThread.this.assignedPartitions = partitions.toArray(new TopicPartition[partitions.size()]);
                    for (TopicPartition par : partitions) {
                        ConsumerLog.info("----------------PartitionsAssigned: " + par.toString());
                        if (offsetMap != null) {
                            String key = par.toString();
                            
                            Long off = null;
                            synchronized (offsetMap) {
                                off = offsetMap.get(key);
                                if (off != null) {
                                    offsetMap.remove(key);
                                }
                            }
                            
                            if (off != null) {
                                ConsumerLog.info("seek {} partition to {}", key, off);
                                consumer.seek(par, off);
                            }
                            
                        }
                    }
                    
                    if(paused) {
                        LOG.info("re-pauseConsumer after partitions re Assigned.");
                        pauseConsumer();
                    }
                }
            });
            
            this.pausedRecords = new LinkedList<ConsumerRecords<K, V>>();
            startTime = System.currentTimeMillis();
            while (running.get()) {
                
                innerHandler1();
            }
            
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (running.get()) {
                LOG.error("", e);
            }

        } catch (Exception e) {
            LOG.error(this.getName() + " abnormal exit due to ", e);
        } finally {
            
            handlePausedCacheDataWhenShutdown();
            
            offsetLogger.printOffsetMap(this.getName());
            LOG.info("'{}' shutdown ok. topics={}. time={}.", 
                    this.getName(), topics.toString(), System.currentTimeMillis());
            
            processingCompleteLatch.countDown();
            
            try {
                // wakeup
                consumer.wakeup();
            } catch (WakeupException e) {
                LOG.info(e.toString());
            }
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.warn("consumer.close() error due to ", e);
            }
            
            waitConsumerCloseLatch.countDown();
        }
        }
    }
    
    
    private void submitRecord(final ConsumerRecords<K, V> records) {
        
        // only for test use
        // if (assignedPartitions != null && assignedPartitions[0].partition() == 7) {
        if(LOG.isTraceEnabled()) { // 调试模式下，打印消息的首尾offset
            int count = records.count();
            LOG.info("submitRecord got {} records.", count);
            int i = 0;
            for (ConsumerRecord<K, V> record : records) {
                if (i == 0 || i == count - 1) {
                    LOG.info("record partion-offset: {}",
                            record.topic() + "-" + record.partition() + ":" + record.offset());
                }
                i++;
            }
        }
        
        if (!records.isEmpty()) {
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    handleConsumerRecords(records);
                }
            };
            future = executorService.submit(task);

            try {
                future.get(sessionTimeoutMs, TimeUnit.MILLISECONDS);
                future = null;
            } catch (InterruptedException e) {
                LOG.info("", e);
                future = null;
            } catch (ExecutionException e) {
                LOG.info("", e);
                future = null;
            } catch (TimeoutException e) {
                // LOG.info("", e1);
                // 超时，启动pause
                LOG.info("pauseConsumer after execute consumerDataHandler {} ms.", sessionTimeoutMs);
                pauseConsumer();
                this.paused = true;
            }
        }
    }
    
    private void handlePausedCacheDataWhenShutdown() {
        if (pausedRecords.isEmpty()) {
            return;
        }
        Runnable task = new Runnable() {
            @Override
            public void run() {
                LOG.info("start to handle pausedRecords when shutdown.");
                for (ConsumerRecords<K, V> recd : pausedRecords) {
                    handleConsumerRecords(recd);
                }
            }
        };
        future = executorService.submit(task);
        try {
            future.get(sessionTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.warn("can not completely handle pausedRecords: " + recordsListToString(pausedRecords), e);
        }
        pausedRecords.clear();
    }
    
    private void handlePausedCacheData(final ConsumerRecords<K, V> records) {
        boolean resumeFlag = false;
        
        if (!records.isEmpty()) {
            addPausedRecords(records, false);
        }

        if (pausedRecords.isEmpty()) {
            resumeFlag = true; // 恢复
        } else {
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    LinkedList<ConsumerRecords<K, V>> precds = 
                            new LinkedList<ConsumerRecords<K, V>>(pausedRecords);
                    pausedRecords.clear();
                    for (ConsumerRecords<K, V> recd : precds) {
                        handleConsumerRecords(recd);
                    }
                }
            };
            future = executorService.submit(task);
            try {
                future.get(sessionTimeoutMs, TimeUnit.MILLISECONDS);
                resumeFlag = true; // 恢复
            } catch (InterruptedException e1) {
                LOG.info("", e1);
                resumeFlag = true; // 恢复
            } catch (ExecutionException e1) {
                LOG.info("", e1);
                resumeFlag = true; // 恢复
            } catch (TimeoutException e1) {
                LOG.info(e1.toString());
                // 不恢复，继续pause
            }
        }

        if (resumeFlag) {
            future = null;
            this.paused = false;
            resumeConsumer();
        }

        // 处理完数据，已恢复或者继续pause
    }
    
    private void innerHandler() {

        final ConsumerRecords<K, V> records = consumer.poll(pollWaitTimeMs);

        // 未暂停
        if (!this.paused) {
            // 上一个task 已处理完
            if (future == null || future.isDone()) {
                submitRecord(records);
                return;
            }
            // 上一个task 未处理完
            else {
                // 不可能，因为任务没处理完（超时）会暂停
                LOG.error("paused=false, but future is not done! that is impossable!");
                return;
            }
        }
        // 已暂停
        else {
            // 上一个task 已处理完
            if (future == null || future.isDone()) {
                handlePausedCacheData(records);
                return;
            }
            // 上一个task 未处理完
            else {
                if (!records.isEmpty()) {
                    LOG.warn("pause Consumer polled {} records.");
                    addPausedRecords(records, true);
                    // 暂停的时候消费，不正常，继续调用pauseConsumer()使其再次暂停
                    //pauseConsumer();
                }
                return;
            }
        }
    }
    
    private void innerHandler1() {

        final ConsumerRecords<K, V> records = consumer.poll(pollWaitTimeMs);
        
        // 上一个task 已处理完
        if (future == null || future.isDone()) {
            // 已暂停
            if(this.paused) {
                handlePausedCacheData(records);
                return;
            } 
            // 未暂停
            else {
                submitRecord(records);
                return;
            }
        }
        // 上一个task 未处理完
        else {
            // 已暂停
            if(this.paused) {
                if (!records.isEmpty()) {
                    addPausedRecords(records, true);
                    // 暂停的时候消费，不正常，继续调用pauseConsumer()使其再次暂停
                    //pauseConsumer();
                }
                return;
            } 
            // 未暂停
            else {
                // 不可能，因为任务没处理完（超时）会暂停
                LOG.error("paused=false, but future is not done! that is impossable!");
                return;
            }
        }

    }
    
    private void addPausedRecords(ConsumerRecords<K, V> records, boolean log) {
        int count = records.count();
        if(log) {
            LOG.info("pause Consumer polled {} records.", count);
        }
        pausePollDataSize = pausePollDataSize + count;
        if (pausePollDataSize > maxCacheRecordsWhenPause) {
            // 缓存不下了，开始清理数据
            LOG.warn("pause poll data Size {} is big than {}, now clear the cache!", 
                    pausePollDataSize, maxCacheRecordsWhenPause);
            pausedRecords.clear();
        }
        pausedRecords.add(records);
    }
    
    @Override
    public void interrupt() {
        this.initiateShutdown();
        this.awaitShutdown();
    }

    public boolean initiateShutdown() {
        if (running.compareAndSet(true, false)) {
            LOG.info("'{}' start to shutdown... topics={}. time={}.", 
                    this.getName(), topics.toString(), System.currentTimeMillis());
            try {
                // wakeup
                consumer.wakeup();
            } catch (WakeupException e) {
                LOG.info(e.toString());
            }
            
//            try { // 打断可能存在的在waiting状态的推送任务
//                super.interrupt();
//            } catch (Exception e) {
//                // ignore
//            }
            
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete
     */
//    public abstract void awaitShutdown();
    
    public void awaitShutdown() {
        try {
            processingCompleteLatch.await();
        } catch (InterruptedException e) {
            LOG.warn("processingCompleteLatch.await() error due to ", e);
        }
        try {
            // 最多等待consumer.close()执行一秒钟
            waitConsumerCloseLatch.await(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("waitConsumerCloseLatch.await() error due to ", e);
        }
        
        if (future != null) {
            future.cancel(true); // 打断可能存在的在waiting状态的推送任务
        }
        
        try {
            executorService.shutdownNow(); // 必须打断sleep
        } catch (Exception e) {
            LOG.warn("executorService.shutdown() error due to ", e);
        }
    }
    
    /**
     * consumer线程消费的topics
     * @return
     */
    public List<String> getTopics() {
        return topics;
    }
    
    /**
     * 线程运行状态
     * @return
     */
    public boolean isRunning() {
        return running.get();
    }
    
    public long getStartTime() {
        return startTime;
    }

    public Map<String, Long> getOffsetMap() {
        return offsetMap;
    }

    public void setOffsetMap(Map<String, Long> offsetMap) {
        this.offsetMap = offsetMap;
    }
    
    public abstract ConsumerDataHandler<K, V> getConsumerDataHandler();
    
    /**
     * consumer.poll(pollWaitTimeMs) 正常抓取数据时，无数据的等待时间
     * @param pollWaitTimeMs
     */
    public void setPollWaitTimeMs(long pollWaitTimeMs) {
        this.pollWaitTimeMs = pollWaitTimeMs;
    }

    /**
     * consumer.pause()时poll消费的数据最大能缓存maxCacheRecordsWhenPause条，超过则丢弃。默认值为512000.<br>
     * 通常情况下，consumer.pause()时，poll是不会消费数据的。
     * 只有当一个组的多个consumer线程启动时，或者分布在不同服务器上的同一组consumer线程部分挂掉时，
     * 才可能会触发group rebalancing，然后pause的poll可能会取到(其他分区的)数据。
     * @param maxCacheRecordsWhenPause
     */
    public void setMaxCacheRecordsWhenPause(int maxCacheRecordsWhenPause) {
        this.maxCacheRecordsWhenPause = maxCacheRecordsWhenPause;
    }
    
    
    // some helpers for this class
    
    private String recordsListToString(List<ConsumerRecords<K, V>> recordsList) {
        StringBuilder sbu = new StringBuilder();
        for(ConsumerRecords<K, V> records: recordsList) {
            sbu.append(recordsToString(records)).append("\n\r");
        }
        return sbu.toString();
    }
    
    private String recordsToString(ConsumerRecords<K, V> records) {
        StringBuilder sbu = new StringBuilder();
        for(ConsumerRecord<K, V> record: records) {
            sbu.append(
                    String.format("key : %s, offset : %s, partitiion : %s, topic : %s\nvalue : %s\n", 
                            record.key(), record.offset(), record.partition(), record.topic(), record.value()));
            sbu.append("\n\r");
        }
        return sbu.toString();
    }

}
