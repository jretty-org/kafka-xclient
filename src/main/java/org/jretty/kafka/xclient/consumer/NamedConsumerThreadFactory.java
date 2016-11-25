package org.jretty.kafka.xclient.consumer;

import java.util.List;

import org.jretty.util.AlgorithmUtils;

public abstract class NamedConsumerThreadFactory<K, V> implements ConsumerThreadFactory<K, V> {
    
    private int threadCount = 0;
    
    protected String getName(String groupId, List<String> topics) {
        ++threadCount;
        StringBuilder sbu = new StringBuilder();
        sbu.append("ConsumerThread-#").append(groupId).append("#-&").append(AlgorithmUtils.shortMsg(topics.toString()))
                .append("&-");
        for (int i = 0; i < 5; i++) {
            sbu.append(threadCount);
        }
        return sbu.toString();
    }

}
