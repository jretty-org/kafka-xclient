package org.jretty.kafka.xclient.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConsumerDataHandler<K, V> implements ConsumerDataHandler<K, V> {
    
    private static final Logger LOG = LoggerFactory.getLogger("FILTER-LOGGER");
    
    private long slowHandleLogTimeMs = 50;

    abstract public void handleKafkaData(ConsumerRecord<K, V> record);

    @Override
    public void handleKafkaData(ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            long start = System.currentTimeMillis();
            handleKafkaData(record);

            if (LOG.isTraceEnabled()) {
                LOG.trace("handleKafkaData(key={}), cost time {} ms. partition-topic:offset is {}-{}:{}", 
                        record.key(), System.currentTimeMillis() - start,
                        record.topic(), record.partition(), record.offset());
            } else {
                long cost = System.currentTimeMillis() - start;
                if (cost > slowHandleLogTimeMs) {
                    LOG.trace("handleKafkaData(key={}), cost time {} ms. partition-topic:offset is {}-{}:{}", 
                            record.key(), cost, record.topic(), record.partition(), record.offset());
                }
            }
        }
    }

    public long getSlowHandleLogTimeMs() {
        return slowHandleLogTimeMs;
    }

    public void setSlowHandleLogTimeMs(long slowHandleLogTimeMs) {
        this.slowHandleLogTimeMs = slowHandleLogTimeMs;
    }

}
