package org.jretty.kafka.test.producer;

import java.util.Properties;

import org.jretty.kafka.xclient.producer.DefaultProducerFactory;
import org.jretty.kafka.xclient.producer.ProducerTemplate;
import org.jretty.util.AlgorithmUtils;
import org.jretty.util.DateFormatUtils;
import org.jretty.util.ThreadUtils;

public class ProducerTest {

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        Properties producerProps = new Properties();

        producerProps.put("acks", "all");
        producerProps.put("retries", "0");

        producerProps.put("buffer.memory", "33554432"); // default 32MB

        producerProps.put("max.block.ms", "10000");

        producerProps.put("bootstrap.servers", "172.16.1.164:9092,172.16.1.165:9092,172.16.1.166:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        DefaultProducerFactory<String, String> fa = new DefaultProducerFactory(producerProps);

        ProducerTemplate<String, String> publisher = new ProducerTemplate<>(fa);

        String topicName = "ZTEST2";
        publisher.setDefaultTopic(topicName);

        int total = 100;
        while (total-- > 0) {
            for (int i = 0; i < 10; i++) {
                String val = AlgorithmUtils.shortMsg(DateFormatUtils.getUniqueDatePattern_TimeMillis_noSplit() + i);
                for(int j=0; j<10; j++){
                    val += "-" + val; 
                }
                
                publisher.sendDefault(DateFormatUtils.getUniqueDatePattern_TimeMillis_noSplit() + i, val.substring(0, 5000));
            }
            ThreadUtils.sleepThread(10L);
        }
        
        publisher.close();

    }
}