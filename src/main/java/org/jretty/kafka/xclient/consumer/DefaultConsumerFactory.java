package org.jretty.kafka.xclient.consumer;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.jretty.kafka.common.CommonKafkaConfig;

/**
 * The {@link ConsumerFactory} implementation to produce a new {@link Consumer} instance
 * for provided {@link Map} {@code configs} and optional {@link Deserializer} {@code keyDeserializer},
 * {@code valueDeserializer} implementations on each {@link #createConsumer()}
 * invocation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 */
public class DefaultConsumerFactory<K, V> extends CommonKafkaConfig implements ConsumerFactory<K, V> {

	private Deserializer<K> keyDeserializer;

	private Deserializer<V> valueDeserializer;

	public DefaultConsumerFactory(Map<?, ?> configs) {
		this(configs, null, null);
	}

	public DefaultConsumerFactory(Map<?, ?> configs,
			Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer) {
		super(configs);
		this.keyDeserializer = keyDeserializer;
		this.valueDeserializer = valueDeserializer;
	}

	public void setKeyDeserializer(Deserializer<K> keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
	}

	public void setValueDeserializer(Deserializer<V> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}

	@Override
	public Consumer<K, V> createConsumer() {
		return createKafkaConsumer();
	}

	protected KafkaConsumer<K, V> createKafkaConsumer() {
		return new KafkaConsumer<K, V>(getConfigs(), this.keyDeserializer, this.valueDeserializer);
	}

	@Override
	public boolean isAutoCommit() {
		return true;
	}

}
