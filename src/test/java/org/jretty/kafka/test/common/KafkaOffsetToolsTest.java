package org.jretty.kafka.test.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jretty.kafka.common.KafkaOffsetTools;

public class KafkaOffsetToolsTest {
    
    public static void main(String[] args) {
        String topic = "ZTEST2";
        int port = 9092;
        List<String> seedBrokers = new ArrayList<String>();
        seedBrokers.add("172.16.1.164");
        seedBrokers.add("172.16.1.165");
        seedBrokers.add("172.16.1.166");
        
        Map<String, Integer> brokers = new HashMap<String, Integer>(seedBrokers.size());
        for(String seed: seedBrokers) {
            brokers.put(seed, port);
        }

        Map ret = KafkaOffsetTools.getLargestPartitionOffset(brokers, topic);
        System.out.println(ret);
        System.out.println("-----------------------------");
        ret = KafkaOffsetTools.getSmallestPartitionOffset(brokers, topic);
        System.out.println(ret);
        
        //KafkaOffsetTools.checkAndCreateTopic(brokers, Collections.singletonList(topic));
        
    }
    
    public static void printMap(Map map) {
        Set keys = map.keySet();
        for (Object o : keys) {
            System.out.println(o + ":" + map.get(o));
        }
    }

}
