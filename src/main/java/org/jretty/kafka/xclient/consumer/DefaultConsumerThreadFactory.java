package org.jretty.kafka.xclient.consumer;

import java.util.List;

public class DefaultConsumerThreadFactory<K, V> extends NamedConsumerThreadFactory<K, V> {

    
    private ConsumerDataHandler<K, V> consumerDataHandler;

    public DefaultConsumerThreadFactory(ConsumerDataHandler<K, V> consumerDataHandler) {
        super();
        this.consumerDataHandler = consumerDataHandler;
    }

    @Override
    public ConsumerThread<K, V> newThread(String groupId, List<String> topics, ConsumerFactory<K, V> consumerFactory) {

        return new ConsumerThread<K, V>(getName(groupId, topics), topics, consumerFactory, consumerDataHandler);
    }

    public ConsumerDataHandler<K, V> getConsumerDataHandler() {
        return consumerDataHandler;
    }

    public void setConsumerDataHandler(ConsumerDataHandler<K, V> consumerDataHandler) {
        this.consumerDataHandler = consumerDataHandler;
    }

}