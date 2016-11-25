package org.jretty.kafka.test.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jretty.kafka.test.common.KafkaOffsetToolsTest;
import org.jretty.kafka.test.consumer.ConsumerDataHandlerDemo;
import org.jretty.kafka.xclient.consumer.ConsumerFactory;
import org.jretty.kafka.xclient.consumer.ConsumerGroupHandle;
import org.jretty.kafka.xclient.consumer.ConsumerGroupManager;
import org.jretty.kafka.xclient.consumer.ConsumerLog;
import org.jretty.kafka.xclient.consumer.ConsumerThread;
import org.jretty.kafka.xclient.consumer.ConsumerThreadFactory;
import org.jretty.kafka.xclient.consumer.ConsumerTopicsGroupConfig;
import org.jretty.kafka.xclient.consumer.DefaultConsumerFactory;
import org.jretty.kafka.xclient.consumer.DefaultConsumerThreadFactory;
import org.jretty.util.ThreadUtils;

public class ConsumerTest {
    
    public static void main(String[] args) throws InterruptedException, IOException {
        test();
    }

    private static ConsumerGroupHandle createConsumerGroupManager() {
        long start = System.currentTimeMillis();
        System.out.println(start);
        Properties consumerProps = new Properties();
        
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "15000");
        
        consumerProps.put("fetch.min.bytes", "1");
        consumerProps.put("fetch.max.wait.ms", "1800");
        
        consumerProps.put("max.partition.fetch.bytes", "1048576"); // default 1048576
        long sessionTimeoutMs = 30000;
        consumerProps.put("session.timeout.ms", String.valueOf(sessionTimeoutMs));
        consumerProps.put("request.timeout.ms", String.valueOf(sessionTimeoutMs + 5000));
        
        consumerProps.put("bootstrap.servers", "172.16.1.164:9092,172.16.1.165:9092,172.16.1.166:9092");
//        consumerProps.put("bootstrap.servers", "172.16.1.232:9092,172.16.1.233:9092,172.16.1.234:9092");
        consumerProps.put("group.id", "ztest-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        
        List<String> topics = new ArrayList<String>();
        topics.add("ZTEST2");
//        topics.add("gdcp.rt.type.gps");//APP1_alarm
//        topics.add("gdcp.rt.type.travel");
//        topics.add("gdcp.rt.type.obd");
        
        ConsumerFactory<String, String> consumerFactory = new DefaultConsumerFactory<String, String>(consumerProps);
        ConsumerThreadFactory<String, String> consumerThreadFactory = new DefaultConsumerThreadFactory<String, String>(new ConsumerDataHandlerDemo<String, String>());
        
        ConsumerTopicsGroupConfig<String, String> topicsGroupConfig = new ConsumerTopicsGroupConfig<>(topics, 8);
        topicsGroupConfig.setConsumerFactory(consumerFactory);
        topicsGroupConfig.setConsumerThreadFactory(consumerThreadFactory);
        
        ConsumerGroupManager consumerGroupManager = new ConsumerGroupManager(consumerProps.getProperty("group.id"));
        consumerGroupManager.setConsumerTopicsGroupConfig(topicsGroupConfig);
        
        Map<String, Long> offsetMap = new HashMap<String, Long>();
        offsetMap.put("ZTEST2-7", 0L);
        offsetMap.put("ZTEST2-6", 0L);
        offsetMap.put("ZTEST2-5", 0L);
        offsetMap.put("ZTEST2-4", 0L);
        offsetMap.put("ZTEST2-3", 0L);
        offsetMap.put("ZTEST2-2", 0L);
        offsetMap.put("ZTEST2-1", 0L);
        offsetMap.put("ZTEST2-0", 0L);
        
//        offsetMap = OffsetFileTools.parseLastOffset("./logs/offset2.log");
//        offsetMap = ConsumerLog.getNewStartOffset();
        if(!offsetMap.isEmpty()) {
            KafkaOffsetToolsTest.printMap(offsetMap);
//            consumerGroupManager.setOffsetMap(offsetMap);
        }
        
        return consumerGroupManager;
    }
    
    public static void test() {
        final ConsumerGroupHandle consumerGroupManager = createConsumerGroupManager();
        
        // 启动 Consumer Group Threads，开始工作
        consumerGroupManager.start(new Runnable() {
            @Override
            public void run() {
                System.out.println("started!!!!");
            }
        });
        
        System.out.println("------------------------------------");
        System.out.println("------------------------------------");
        System.out.println("------------------------------------");
        ThreadUtils.sleepThread(150000);
        List<ConsumerThread> threadList = consumerGroupManager.getConsumerThreadList();
        for (ConsumerThread consumer : threadList) {
            System.out.println(consumer.getName() + "-" + consumer.getId() + ": STATE=" + consumer.getState());
        }

        // stop this consumer group
        consumerGroupManager.shutdown(new Runnable() {
            @Override
            public void run() {
                System.out.println("Start to flushLastOffset...");
                ConsumerLog.flushLastOffset();
            }
        });
    }

}
