package org.jretty.kafka.xclient.consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerThread<K, V> extends AbstractConsumerThread<K, V> {
    
    private final ConsumerDataHandler<K, V> consumerDataHandler;
    
    private long slowHandleLogTimeMs = 5000;

    
    public ConsumerThread(final String threadName, List<String> topics, 
            ConsumerFactory<K, V> consumerFactory,
            ConsumerDataHandler<K, V> consumerDataHandler) {
        super(threadName, topics, consumerFactory);
        
        this.consumerDataHandler = consumerDataHandler;
    }

    
    protected void handleConsumerRecords(ConsumerRecords<K, V> records) {
        long start = System.currentTimeMillis();
        Integer count = null;
        if (LOG.isDebugEnabled()) {
            count = records.count();
            LOG.debug("handleConsumerRecords got {} records.", count);
        }
        
        offsetLogger.logOffset(records);
        
        try {
            consumerDataHandler.handleKafkaData(records);
        } catch (Exception e) {
            LOG.error("handlePollData() error due to ", e);
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("handlePollData {} records cost time {} ms.", count, System.currentTimeMillis() - start);
        } else {
            long cost = System.currentTimeMillis() - start;
            if (cost > slowHandleLogTimeMs) {
                LOG.info("handlePollData {} records cost time {} ms.", 
                        count != null ? count : records.count(), cost);
            }
        }
    }
    
//    @Override
//    public void awaitShutdown() {
//        awaitShutdownInner();
//        try {
//            executorService.shutdownNow(); // 必须打断sleep
//        } catch (Exception e) {
//            LOG.warn("executorService.shutdown() error due to ", e);
//        }
//    }
    
    @Override
    public ConsumerDataHandler<K, V> getConsumerDataHandler() {
        return consumerDataHandler;
    }
    
    /**
     * 处理一次poll数据的时间超过slowHandleLogTimeMs，会记录下日志。
     * @param slowHandleLogTimeMs
     */
    public void setSlowHandleLogTimeMs(long slowHandleLogTimeMs) {
        this.slowHandleLogTimeMs = slowHandleLogTimeMs;
    }

}
