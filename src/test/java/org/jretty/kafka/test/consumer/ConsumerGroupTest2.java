package org.jretty.kafka.test.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jretty.kafka.test.common.KafkaOffsetToolsTest;
import org.jretty.kafka.xclient.consumer.ConsumerDataHandler;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroupTest2 {
    
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupTest2.class);

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
        consumerProps.put("session.timeout.ms", "30000");
        
//        consumerProps.put("bootstrap.servers", "172.16.1.164:9092,172.16.1.165:9092,172.16.1.166:9092");
        consumerProps.put("bootstrap.servers", "172.16.1.182:9092,172.16.225.4:9092,172.16.225.5:9092");
//        consumerProps.put("bootstrap.servers", "172.16.1.232:9092,172.16.1.233:9092,172.16.1.234:9092");
        consumerProps.put("group.id", "KOPKT");
//        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        
        List<String> topics = new ArrayList<String>();
//        topics.add("ZTEST2");
        topics.add("gdcp.rt.type.gps");
        topics.add("gdcp.rt.type.travel");
        topics.add("gdcp.rt.type.obd");
        
        ConsumerFactory<String, byte[]> consumerFactory = new DefaultConsumerFactory<String, byte[]>(consumerProps, 
                new StringDeserializer(), new ByteArrayDeserializer());
        ConsumerThreadFactory<String, byte[]> consumerThreadFactory = new DemoConsumerThreadFactory();
        
        ConsumerTopicsGroupConfig<String, byte[]> topicsGroupConfig = new ConsumerTopicsGroupConfig<>(topics, 8);
        topicsGroupConfig.setConsumerFactory(consumerFactory);
        topicsGroupConfig.setConsumerThreadFactory(consumerThreadFactory);
        
        
        List<String> topics2 = new ArrayList<String>();
        topics2.add("stream.gatherdata.hour");
        topics2.add("stream.gatherdata.daily");
      
        ConsumerFactory<String, String> consumerFactory2 = new DefaultConsumerFactory<String, String>(consumerProps, 
                new StringDeserializer(), new StringDeserializer());
        ConsumerThreadFactory<String, String> consumerThreadFactory2 = new DefaultConsumerThreadFactory<String, String>(new ConsumerDataHandlerDemo<String, String>());
        
        ConsumerTopicsGroupConfig<String, String> topicsGroupConfig2 = new ConsumerTopicsGroupConfig<>(topics2, 8);
        topicsGroupConfig2.setConsumerFactory(consumerFactory2);
        topicsGroupConfig2.setConsumerThreadFactory(consumerThreadFactory2);
        
        
        List<ConsumerTopicsGroupConfig> consumerTopicsGroupConfigs = new ArrayList<ConsumerTopicsGroupConfig>(2);
        consumerTopicsGroupConfigs.add(topicsGroupConfig);
        consumerTopicsGroupConfigs.add(topicsGroupConfig2);
        ConsumerGroupManager consumerGroupManager = new ConsumerGroupManager(consumerProps.getProperty("group.id"));
        consumerGroupManager.setConsumerTopicsGroupConfigs(consumerTopicsGroupConfigs);
        
        Map<String, Long> offsetMap = new HashMap<String, Long>();
//        offsetMap.put("ZTEST2-7", 206L);
//        offsetMap.put("ZTEST2-6", 413L);
//        offsetMap.put("ZTEST2-5", 620L);
//        offsetMap.put("ZTEST2-4", 206L);
//        offsetMap.put("ZTEST2-3", 413L);
//        offsetMap.put("ZTEST2-2", 620L);
//        offsetMap.put("ZTEST2-1", 206L);
//        offsetMap.put("ZTEST2-0", 413L);
        
        offsetMap = ConsumerLog.getNewStartOffset();
        if(!offsetMap.isEmpty()) {
            KafkaOffsetToolsTest.printMap(offsetMap);
            consumerGroupManager.setOffsetMap(offsetMap);
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
        ThreadUtils.sleepThread(114000);

        System.out.println("------------------------------------");

        // stop this consumer group
        consumerGroupManager.shutdown(new Runnable() {
            @Override
            public void run() {
                
                List<ConsumerThread> consumerThreadList = consumerGroupManager.getConsumerThreadList();
                for(ConsumerThread thread: consumerThreadList){
                    ConsumerDataHandler handler = thread.getConsumerDataHandler();
                    if(handler instanceof TestGdcpRTDataHandler) {
                        TestGdcpRTDataHandler thandler = (TestGdcpRTDataHandler) handler;
                        LOG.info("thread {} handled {} records, size={}, cost time {} ms.",
                                thread.getName(), thandler.count, thandler.size, thandler.time);
                    }
                }
                
                System.out.println("Start to flushLastOffset...");
                ConsumerLog.flushLastOffset();
            }
        });
    }

}