package org.jretty.kafka.xclient.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jretty.kafka.common.KafkaOffsetTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jretty.util.StringSplitUtils;
import org.jretty.util.ThreadUtils;

@SuppressWarnings("rawtypes")
public class ConsumerGroupManager implements ConsumerGroupHandle {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupManager.class);

    private final AtomicBoolean running = new AtomicBoolean(false);

    protected final String groupId;

    private List<ConsumerTopicsGroupConfig> consumerTopicsGroupConfigs;
    
    private List<ConsumerThread> consumerThreadHandle;

    protected Map<String, Long> offsetMap;

    /**
     * @param groupId
     *            the unique ID of the Consumer Group
     */
    public ConsumerGroupManager(String groupId) {
        this.groupId = groupId;
    }
    
    public ConsumerGroupManager(String groupId, ConsumerTopicsGroupConfig consumerTopicsGroupConfig) {
        this(groupId, Collections.singletonList(consumerTopicsGroupConfig));
    }

    public ConsumerGroupManager(String groupId, List<ConsumerTopicsGroupConfig> consumerTopicsGroupConfigs) {
        super();
        this.consumerTopicsGroupConfigs = consumerTopicsGroupConfigs;
        this.groupId = groupId != null ? groupId
                : (String) this.consumerTopicsGroupConfigs.get(0).getConsumerFactory().getConfigs().get("group.id");
        
        if(this.groupId==null) {
            throw new IllegalArgumentException("groupid is null, and can't find it from consumerConfig!");
        }
    }

    /**
     * Asynchronous start the Consumer Group Thread
     * 
     * @param callback
     *            excute the callback after start finished
     */
    @Override
    public void start(final Runnable callback) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startConsumerGroup(callback);
                } catch (Throwable e) {
                    LOG.error("", e);
                }
            }
        }).start();
    }

    /**
     * Asynchronous add ConsumerThreads
     * 
     * @param consumerThreadNum
     *            the num of the added thread
     * @param callback
     *            excute the callback after add finished
     */
    @Override
    public void addConsumerThread(final int consumerThreadNum, final Runnable callback) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // 添加线程
                    ConsumerGroupManager.this.addConsumerThreadInner(consumerThreadNum, callback);
                } catch (Throwable e) {
                    LOG.error("", e);
                }
            }
        }).start();
    }

    /**
     * Stop all Threads of the Consumer Group
     */
    @Override
    public synchronized void shutdown(final Runnable callback) {
        ConsumerStopListener listener = new ConsumerStopListener(callback);
        int threadNum = this.getConsumerThreadList().size();
        listener.setThreadNum(threadNum);

        stopConsumerGroupThreadSync(listener);
    }
    
    
    protected synchronized void startConsumerGroup(final Runnable callback) {
        if (!running.get()) {

            long start = System.currentTimeMillis();
            LOG.info("Start ConsumerGroupManager ... ...groupId={}, topicGroupNum={}", 
                    groupId, consumerTopicsGroupConfigs.size());

            // 先检测启动一遍，检测完之后自动停止
            this.checkAndCreateTopic();

            this.startConsumerThread(null);

            LOG.info("ConsumerGroupManager started in {} ms. groupId={}, topicGroupNum={}",
                    System.currentTimeMillis() - start, groupId, consumerTopicsGroupConfigs.size());

            running.compareAndSet(false, true);

            if (callback != null) {
                try {
                    callback.run();
                } catch (Throwable e) {
                    LOG.error("", e);
                }
            }

        } else {
            LOG.info("ConsumerGroupManager is started, don't repeat start!");
        }
    }
    

    protected synchronized void stopConsumerGroupThreadSync(final ConsumerStopListener listener) {
        if (running.get()) {
            long start = System.currentTimeMillis();
            LOG.info("Stop ConsumerGroupManager ... ...groupId={}", groupId);

            // 关闭所有consumerThread
            for (ConsumerThread consumerThread : consumerThreadHandle) {
                consumerThread.initiateShutdown();
            }

            boolean threadStateRunning = false;
            for (ConsumerThread consumerThread : consumerThreadHandle) {
                consumerThread.awaitShutdown();
                ThreadUtils.sleepThread(200); // 等线程结束
                if (consumerThread.getState() != Thread.State.TERMINATED) {
                    threadStateRunning = true;
                    LOG.warn("{} - Thread [{}] is not stop, its state is {}! now try to force destroy it...", groupId,
                            consumerThread.getName(), consumerThread.getState());
                    ThreadUtils.stopThread(consumerThread);
                }

                listener.run();
            }

            consumerThreadHandle.clear();
            consumerThreadHandle = null;
            if (!threadStateRunning) {
                LOG.info("all consumers shutdown in {} ms.groupId={}", 
                        System.currentTimeMillis() - start, groupId);
            }

            running.compareAndSet(true, false);
        }
    }

    
    protected synchronized void addConsumerThreadInner(int consumerThreadNum, final Runnable callback) {
        if (!running.get()) {
            LOG.warn("ConsumerGroupManager is not started yet, pls start is first!");
            return;
        }
        long start = System.currentTimeMillis();
        LOG.info("Start to add ConsumerThread ... ...groupId={}, threadNum={}", groupId, consumerThreadNum);

        this.startConsumerThread(consumerThreadNum);

        LOG.info("ConsumerThread added and started in {} ms. groupId={}, threadNum={}", 
                System.currentTimeMillis() - start, groupId, consumerThreadNum);

        if (callback != null) {
            try {
                callback.run();
            } catch (Throwable e) {
                LOG.error("", e);
            }
        }
    }
    
    
    @SuppressWarnings("unchecked")
    protected void checkAndCreateTopic() {
        List<String> allTopics = new ArrayList<String>();
        for (ConsumerTopicsGroupConfig config : consumerTopicsGroupConfigs) {
            allTopics.addAll(config.getTopics());
        }
        
        String serversStr = consumerTopicsGroupConfigs.get(0).getConsumerFactory().getBrokerUrl();
        
        String[] serverArr = StringSplitUtils.splitIgnoreEmpty(serversStr, ',');
        Map<String, Integer> serverMap = new HashMap<String, Integer>(serverArr.length);
        for (String stmp : serverArr) {
            String[] tmparr = StringSplitUtils.splitIgnoreEmpty(stmp, ':');
            serverMap.put(tmparr[0], Integer.valueOf(tmparr[1]));
        }
        KafkaOffsetTools.checkAndCreateTopic(serverMap, allTopics);
        ThreadUtils.sleepThread(1000);
    }
    

    @SuppressWarnings("unchecked")
    protected void startConsumerThread(Integer consumerThreadNum) {
        if (consumerThreadHandle == null) {
            consumerThreadHandle = new ArrayList<ConsumerThread>();
        }

        for (ConsumerTopicsGroupConfig config : consumerTopicsGroupConfigs) {
            int newThreadNum = consumerThreadNum == null ? config.getThreadNum() : consumerThreadNum;

            for (int i = 0; i < newThreadNum; i++) {
                ConsumerThread consumerThread = config.getConsumerThreadFactory().newThread(
                        groupId, config.getTopics(), config.getConsumerFactory());

                if (offsetMap != null) {
                    consumerThread.setOffsetMap(offsetMap);
                }

                // 将线程句柄保存起来
                consumerThreadHandle.add(consumerThread);
            }
        }
        
        for(ConsumerThread consumerThread: consumerThreadHandle) {
            // 启动push线程
            consumerThread.start();
        }

    }

    /**
     * get the ConsumerThread list
     * 
     * @return
     */
    @Override
    public List<ConsumerThread> getConsumerThreadList() {
        return consumerThreadHandle;
    }

    /**
     * if the consumer group is started
     * 
     * @return
     */
    @Override
    public boolean isRunning() {
        return running.get();
    }


    public Map<String, Long> getOffsetMap() {
        return offsetMap;
    }
    
    /**
     * @param offsetMap
     *            设置topic-partition的初始化offset，<br>
     *            例如 TEST-1:22，代表设置TEST这个topic的partition 1的offset为22
     */
    public void setOffsetMap(Map<String, Long> offsetMap) {
        this.offsetMap = offsetMap;
    }

    public List<ConsumerTopicsGroupConfig> getConsumerTopicsGroupConfigs() {
        return consumerTopicsGroupConfigs;
    }

    public void setConsumerTopicsGroupConfigs(List<ConsumerTopicsGroupConfig> consumerTopicsGroupConfigs) {
        this.consumerTopicsGroupConfigs = consumerTopicsGroupConfigs;
    }
    
    public void setConsumerTopicsGroupConfig(ConsumerTopicsGroupConfig consumerTopicsGroupConfig) {
        this.consumerTopicsGroupConfigs = Collections.singletonList(consumerTopicsGroupConfig);
    }

}