package org.jretty.kafka.test.consumer;

import java.util.List;

import org.jretty.kafka.xclient.consumer.ConsumerFactory;
import org.jretty.kafka.xclient.consumer.ConsumerThread;
import org.jretty.kafka.xclient.consumer.NamedConsumerThreadFactory;

public class DemoConsumerThreadFactory extends NamedConsumerThreadFactory<String, byte[]> {

    @Override
    public ConsumerThread<String, byte[]> newThread(String groupId, List<String> topics,
            ConsumerFactory<String, byte[]> consumerFactory) {

        return new ConsumerThread<String, byte[]>(getName(groupId, topics), topics, consumerFactory,
                new TestGdcpRTDataHandler<String, byte[]>());
    }

}