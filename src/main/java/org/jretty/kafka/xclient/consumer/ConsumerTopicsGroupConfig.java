package org.jretty.kafka.xclient.consumer;

import java.util.List;

public class ConsumerTopicsGroupConfig<K, V> {

    private List<String> topics;
    private int threadNum;
    private ConsumerFactory<K, V> consumerFactory;
    private ConsumerThreadFactory<K, V> consumerThreadFactory;
    
    public ConsumerTopicsGroupConfig() {
    }
    
    public ConsumerTopicsGroupConfig(List<String> topics, int threadNum) {
        super();
        this.topics = topics;
        this.threadNum = threadNum;
    }

    public ConsumerTopicsGroupConfig(List<String> topics, int threadNum, ConsumerFactory<K, V> consumerFactory,
            ConsumerThreadFactory<K, V> consumerThreadFactory) {
        super();
        this.topics = topics;
        this.threadNum = threadNum;
        this.consumerFactory = consumerFactory;
        this.consumerThreadFactory = consumerThreadFactory;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public ConsumerFactory<K, V> getConsumerFactory() {
        return consumerFactory;
    }

    public void setConsumerFactory(ConsumerFactory<K, V> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public ConsumerThreadFactory<K, V> getConsumerThreadFactory() {
        return consumerThreadFactory;
    }

    public void setConsumerThreadFactory(ConsumerThreadFactory<K, V> consumerThreadFactory) {
        this.consumerThreadFactory = consumerThreadFactory;
    }
}