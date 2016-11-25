package org.jretty.kafka.xclient.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * the CallbackData of KafkaMsgPublisher producer
 */
public final class CallbackData<K, V> {

    private final ProducerRecord<K, V> record;

    private final RecordMetadata metadata;

    public CallbackData(ProducerRecord<K, V> record, RecordMetadata metadata) {
        this.record = record;
        this.metadata = metadata;
    }

    public ProducerRecord<K, V> getRecord() {
        return this.record;
    }

    public RecordMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public String toString() {
        String topic = null;
        Object key = null;
        Object value = null;
        Object partition = null;
        Object offset = null;
        if (record != null) {
            topic = record.topic();
            key = record.key();
            value = record.value();
            partition = record.partition();
        }
        if (metadata != null) {
            partition = metadata.partition();
            offset = metadata.offset();
        }
        return "CallbackData(topic = " + topic + ", partition = " + partition + ", offset = " + offset + ", key = "
                + key + ", value = " + value + ")";
    }

}