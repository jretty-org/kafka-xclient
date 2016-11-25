/* 
 * Copyright (C) 2016-2017 the CSTOnline Technology Co.
 * Create by ZollTy on 2016-5-7 (zoutianyong@cstonline.com)
 */
package org.jretty.kafka.xclient.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * 处理需要被发送的数据<br>
 * 
 * 注意：该实例对象会被多线程使用，务必保证线程安全，不建议在实例内部定义任何可变变量。
 * 可以定义 final 声明的变量，或者 synchronized 的变量
 * 
 * @author zollty
 * @since 2016-5-7
 */
public interface ConsumerDataHandler<K, V> {
    
    /**
     * 处理需要被发送的数据<br>
     * For example:<pre>
     *  for (ConsumerRecord<String, Strin> record: records) {
     *      // print the key and value
     *      System.out.println(record.key() + record.value());
     *      // Continue processing ...
     *  }
     * </pre>
     * 
     * 注意，该方法内部做好异常处理，不要抛出异常。假如有异常，则在该方法内部处理（记录日志等等）
     * 
     * @param recordList kafka data
     */
    public void handleKafkaData(ConsumerRecords<K, V> records);

}