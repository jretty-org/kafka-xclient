/* 
 * Copyright (C) 2016-2017 the CSTOnline Technology Co.
 * Create by ZollTy on 2016年6月4日 (zoutianyong@cstonline.com)
 */
package org.jretty.kafka.test.consumer;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jretty.kafka.xclient.consumer.AbstractConsumerDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jretty.util.ThreadUtils;

/**
 * 
 * @author zollty
 * @since 2016年6月4日
 */
public class TestGdcpRTDataHandler<K, V> extends AbstractConsumerDataHandler<String, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(TestGdcpRTDataHandler.class);

    public AtomicLong count = new AtomicLong(0);
    public AtomicLong size = new AtomicLong(0);
    public AtomicLong time = new AtomicLong(0);
    
    @Override
    public void handleKafkaData(ConsumerRecord<String, byte[]> record) {
        long start = System.currentTimeMillis();
        count.incrementAndGet();
        size.addAndGet(record.value().length);
        LOG.trace("Recieve gdcp kafka data, size = {} bytes. key = {}", record.value().length, record.key());
        
        ThreadUtils.sleepThread(50L);
        time.addAndGet(System.currentTimeMillis()-start);
    }

}
