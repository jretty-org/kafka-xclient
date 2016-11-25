package org.jretty.kafka.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaOffsetTools {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetTools.class);
    
    private static Integer soTimeoutMS = 10000;
    
    private static Integer bufferSize = 64 * 1024;
    
    public static Map<String, Long> getLargestPartitionOffset(Map<String, Integer> brokers, String topic) {
        return getPartitionOffset(brokers, topic, -1);
    }
    
    public static Map<String, Long> getSmallestPartitionOffset(Map<String, Integer> brokers, String topic) {
        return getPartitionOffset(brokers, topic, -2);
    }
    
    private static Map<String, Long> getPartitionOffset(Map<String, Integer> brokers, 
            String topic, long whichTime) {
        Map<String, Long> ret = new HashMap<String, Long>();

        TreeMap<Integer, PartitionMetadata> metadatas = findLeader(brokers, topic);

        int sum = 0;
        for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
            int partition = entry.getKey();
            String leadBroker = entry.getValue().leader().host();
            long readOffset = getLastOffset(leadBroker, entry.getValue().leader().port(), 
                    topic, partition, whichTime);
            sum += readOffset;

            ret.put(topic + "-" + partition, readOffset);

        }
        LOG.info("topic( {} ) records sum: {}", topic, sum);

        return ret;
    }
    
    public static long getLastOffset(String leadBrokerHost, int port, String topic, 
            int partition, long whichTime) {
        String clientName = "Client_" + topic + "_" + partition;
        SimpleConsumer consumer = new SimpleConsumer(leadBrokerHost, port, 
                soTimeoutMS, bufferSize, clientName);
        
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = 
                new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            LOG.warn("Error fetching data Offset Data the Broker. Reason: " 
                    + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.error("SimpleConsumer.close() error due to ", e);
            }
        }
        
        return offsets.length > 0 ? offsets[0] : -1;
    }
    
    public static void checkAndCreateTopic(Map<String, Integer> brokers, List<String> topics) {
        LOG.debug("Start to checkAndCreateTopics topics={}", topics);
        for (Map.Entry<String, Integer> seed : brokers.entrySet()) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed.getKey(), seed.getValue(), soTimeoutMS, bufferSize,
                        "checkAndCreateTopics" + System.currentTimeMillis());
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                consumer.send(req);
            } catch (Exception e) {
                LOG.error("Error communicating with Broker [" + seed + "] to get metadata for [" + topics + ", ] Reason: ",
                        e);
            } finally {
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (Exception e) {
                        LOG.error("SimpleConsumer.close() error due to ", e);
                    }
                }
            }
        }
        LOG.debug("End to checkAndCreateTopics topics={}", topics);
    }

    public static TreeMap<Integer, PartitionMetadata> findLeader(Map<String, Integer> brokers, String topic) {
        TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();
        for (Map.Entry<String, Integer> seed : brokers.entrySet()) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed.getKey(), seed.getValue(), 
                        soTimeoutMS, bufferSize, "leaderLookup" + System.currentTimeMillis());
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        map.put(part.partitionId(), part);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error communicating with Broker [" + seed.getKey() + "] to get metadata for [" + topic
                        + ", ] Reason: ", e);
            } finally {
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (Exception e) {
                        LOG.error("SimpleConsumer.close() error due to ", e);
                    }
                }
            }
        }
        
        return map;
    }

}