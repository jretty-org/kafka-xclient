package org.jretty.kafka.test.perf;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.jretty.kafka.xclient.producer.DefaultProducerFactory;
import org.jretty.kafka.xclient.producer.ProducerTemplate;
import org.jretty.util.ThreadUtils;

public class ProducerTest {
    
    private final static CountDownLatch closeLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        Properties producerProps = new Properties();

        producerProps.put("acks", "all");
        producerProps.put("retries", "0");

        producerProps.put("buffer.memory", "33554432"); // default 32MB

        producerProps.put("max.block.ms", "10000");

//        producerProps.put("bootstrap.servers", "172.16.1.164:9092,172.16.1.165:9092,172.16.1.166:9092");
        producerProps.put("bootstrap.servers", "172.16.1.232:9092,172.16.1.233:9092,172.16.1.234:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        DefaultProducerFactory<String, String> fa = new DefaultProducerFactory(producerProps);

        final ProducerTemplate<String, String> publisher = new ProducerTemplate<>(fa, true);

        String topicName = "ZTEST2";
        publisher.setDefaultTopic(topicName);

        final int total = 1000000;
        final String appid = "Bbcdefghijkl_";
        long start0 = System.currentTimeMillis();
        new Thread(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                for (int i = 0; i < total; i++) {
                    publisher.sendDefault(appid + i%18, "abcdefghijabcdefghijabcdefghij0{\"appid\":\"Bbcdefghijkl_0\",\"serialNumber\":0,\"timestamp\":1477895086176}");
                }
                
                System.out.println(total + " item cost time:"+(System.currentTimeMillis() - start));
                
                closeLatch.countDown();
            }
        }).start();
        
        try {
            closeLatch.await();
            System.out.println(total + " 0item cost time:"+(System.currentTimeMillis() - start0));
            ThreadUtils.sleepThread(100);
            publisher.close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       

    }
}