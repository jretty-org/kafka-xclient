package org.jretty.kafka.xclient.producer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

public interface KafkaDataProducer<K, V> {

    /**
     * Send the data to the default topic with no key or partition.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    void sendDefault(V data);

    /**
     * Send the data to the default topic with the provided key and no partition.
     * @param key the key.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    void sendDefault(K key, V data);

    /**
     * Send the data to the default topic with the provided key and partition.
     * @param partition the partition.
     * @param key the key.
     * @param data the data.
     * @return a Future for the {@link SendResult}.
     */
    void sendDefault(int partition, K key, V data);

    /**
     * Send the data to the provided topic with no key or partition.
     * @param topic the topic.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    void send(String topic, V data);

    /**
     * Send the data to the provided topic with the provided key and no partition.
     * @param topic the topic.
     * @param key the key.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    void send(String topic, K key, V data);

    /**
     * Send the data to the provided topic with the provided partition and no key.
     * @param topic the topic.
     * @param partition the partition.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    void send(String topic, int partition, V data);

    /**
     * Send the data to the provided topic with the provided key and partition.
     * @param topic the topic.
     * @param partition the partition.
     * @param key the key.
     * @param data the data.
     * @return a Future for the {@link SendResult}.
     */
    void send(String topic, int partition, K key, V data);

    /**
     * Flush the producer.
     */
    void flush();
    
    List<PartitionInfo> partitionsFor(String topic);

    Map<MetricName, ? extends Metric> metrics();

}
