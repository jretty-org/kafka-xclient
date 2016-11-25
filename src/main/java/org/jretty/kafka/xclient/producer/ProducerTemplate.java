package org.jretty.kafka.xclient.producer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.jretty.kafka.common.SimpleOffsetLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author zollty
 * @since 2016-6-3
 */
public class ProducerTemplate<K, V> implements KafkaDataProducer<K, V> {

    private final Logger logger = LoggerFactory.getLogger("ASYNC-LOGGER");
    private final Logger localLogger = LoggerFactory.getLogger(ProducerTemplate.class);

    private SimpleOffsetLogger offsetLogger = new SimpleOffsetLogger();

    private final ProducerFactory<K, V> producerFactory;
    
    private final boolean autoFlush;

    private volatile Producer<K, V> producer;

    private volatile String defaultTopic;
    
    private Integer loggingSlowMs;
    
    private ProducerListener<K, V> callback = new LoggingProducerListener<K, V>();

    /**
     * Create an instance using the supplied producer factory and autoFlush false.
     * @param producerFactory the producer factory.
     */
    public ProducerTemplate(ProducerFactory<K, V> producerFactory) {
        this(producerFactory, false);
    }

    /**
     * Create an instance using the supplied producer factory and autoFlush setting.
     * @param producerFactory the producer factory.
     * @param autoFlush true to flush after each send.
     */
    public ProducerTemplate(ProducerFactory<K, V> producerFactory, boolean autoFlush) {
        this.producerFactory = producerFactory;
        this.autoFlush = autoFlush;
    }

    /**
     * The default topic for send methods where a topic is not
     * providing.
     * @return the topic.
     */
    public String getDefaultTopic() {
        return this.defaultTopic;
    }

    /**
     * Set the default topic for send methods where a topic is not
     * providing.
     * @param defaultTopic the topic.
     */
    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }
    
    public ProducerListener<K, V> getCallback() {
        return callback;
    }

    public void setCallback(ProducerListener<K, V> callback) {
        this.callback = callback;
    }
    
    /**
     * Set the time to logging the slow production.
     * @param loggingSlowMs the ms time.
     */
    public void setLoggingSlowMs(int loggingSlowMs) {
        this.loggingSlowMs = loggingSlowMs;
    }

    @Override
    public void sendDefault(V data) {
        send(this.defaultTopic, data);
    }

    @Override
    public void sendDefault(K key, V data) {
        send(this.defaultTopic, key, data);
    }

    @Override
    public void sendDefault(int partition, K key, V data) {
        send(this.defaultTopic, partition, key, data);
    }

    @Override
    public void send(String topic, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
        doSend(producerRecord);
    }

    @Override
    public void send(String topic, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
        doSend(producerRecord);
    }

    @Override
    public void send(String topic, int partition, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, null, data);
        doSend(producerRecord);
    }

    @Override
    public void send(String topic, int partition, K key, V data) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
        doSend(producerRecord);
    }

    @Override
    public void flush() {
        if (this.producer != null) {
            this.producer.flush();
        }
    }
    
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        mayInitProducer();
        return this.producer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        mayInitProducer();
        return this.producer.metrics();
    }
    
    private void mayInitProducer() {
        if (this.producer == null) {
            synchronized (this) {
                if (this.producer == null) {
                    this.producer = this.producerFactory.createProducer();
                }
            }
        }
    }

    /**
     * Send the producer record.
     * @param record the producer record.
     * @return a Future for the {@link RecordMetadata}.
     */
    protected void doSend(final ProducerRecord<K, V> record) {
        mayInitProducer();
        
        final long startTime = System.currentTimeMillis();

        producer.send(record, new Callback() {

            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (metadata != null) {
                    offsetLogger.logOffset(metadata.topic(),
                            metadata.partition(), metadata.offset());
                }

                callback.onCompletion(startTime, new CallbackData<K, V>(record, metadata), e);
            }
        });

        if (this.autoFlush) {
            flush();
        }
        
        if (loggingSlowMs != null || logger.isDebugEnabled()) {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > (loggingSlowMs == null ? 50 : loggingSlowMs)) {
                logger.info("producer.send(topic={}, key={}) slow, cost time {} ms.", record.topic(),
                        String.valueOf(record.key()), cost);
            }
        }
    }

    public void close() {
        if (this.producer != null) {
            synchronized (this) {
                if (this.producer != null) {
                    try {
                        localLogger.info("Start to stop producer....");
                        producer.close();
                    } catch (InterruptException e) {
                        localLogger.info("producer.close() error due to ", e);
                    }
                    offsetLogger.printOffsetMap("KafkaPublisher");
                    this.producer = null;
                }
            }
        }
    }
}