package org.jretty.kafka.xclient.consumer;

import java.util.List;

public interface ConsumerThreadFactory<K, V> {

    ConsumerThread<K, V> newThread(String groupId, List<String> topics, ConsumerFactory<K, V> consumerFactory);

}