package org.jretty.kafka.xclient.consumer;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;

public interface ConsumerFactory<K, V> {
    
    public Map<String, Object> getConfigs();

    public String getBrokerUrl();

    Consumer<K, V> createConsumer();
    
    public boolean isAutoCommit();

}
