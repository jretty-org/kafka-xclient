package org.jretty.kafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jretty.kafka.xclient.consumer.ConsumerDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jretty.util.ThreadUtils;

public class ConsumerDataHandlerDemo<K, V> implements ConsumerDataHandler<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger("FILTER-LOGGER");
    int c = 0;

    @Override
    public void handleKafkaData(ConsumerRecords<K, V> data4Push) {
        StringBuilder jsonData4send = new StringBuilder();
        Long startTime = System.currentTimeMillis();
        LOG.debug("recieve data " + data4Push.count());
        // Handle new records
        int par = -1;
        for (ConsumerRecord<?, ?> record : data4Push) {
            par = record.partition();
            // 1. package data
            jsonData4send.append(
                    "------------------ Received message: (" + record.key() + ", " + record.value() + ") at offset "
                            + record.offset() + " partition=" + record.partition() + " topic=" + record.topic())
                    .append("\n\r");

        }
        c++;
        if (par == 7 && c > 40) {
            LOG.info("start to sleep 1 min.");
            ThreadUtils.sleepThread(60000);
        }
        jsonData4send.append("----------push size: " + data4Push.count() + " cost time: "
                + (System.currentTimeMillis() - startTime));
//         System.out.println(jsonData4send);
        ThreadUtils.sleepThread(500);
    }

}