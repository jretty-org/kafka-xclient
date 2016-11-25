/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jretty.kafka.xclient.producer;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;
import org.jretty.kafka.common.CommonKafkaConfig;

/**
 * The {@link ProducerFactory} implementation for the {@code singleton} shared {@link Producer}
 * instance.
 * <p>
 * This implementation will produce a new {@link Producer} instance
 * for provided {@link Map} {@code configs} and optional {@link Serializer} {@code keySerializer},
 * {@code valueSerializer} implementations on each {@link #createProducer()}
 * invocation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 */
public class DefaultProducerFactory<K, V> extends CommonKafkaConfig implements ProducerFactory<K, V> {

	private static final Log logger = LogFactory.getLog(DefaultProducerFactory.class);

	private volatile Producer<K, V> producer;

	private Serializer<K> keySerializer;

	private Serializer<V> valueSerializer;

	public DefaultProducerFactory(Map<?, ?> configs) {
		this(configs, null, null);
	}

	public DefaultProducerFactory(Map<?, ?> configs, Serializer<K> keySerializer,
			Serializer<V> valueSerializer) {
		super(configs);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}
	
	public void setKeySerializer(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public void setValueSerializer(Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	public void destroy() throws Exception { //NOSONAR
		Producer<K, V> producer = this.producer;
		this.producer = null;
		if (producer != null) {
			producer.close();
		}
	}

	public void stop() {
		try {
			destroy();
		}
		catch (Exception e) {
			logger.error("Exception while stopping producer", e);
		}
	}

	@Override
	public Producer<K, V> createProducer() {
		if (this.producer == null) {
			synchronized (this) {
				if (this.producer == null) {
					this.producer = createKafkaProducer();
				}
			}
		}
		return this.producer;
	}

	protected KafkaProducer<K, V> createKafkaProducer() {
		return new KafkaProducer<K, V>(getConfigs(), this.keySerializer, this.valueSerializer);
	}

}
