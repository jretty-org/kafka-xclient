package org.jretty.kafka.xclient.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetLogger {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetLogger.class);

    // ~~~~~~~~~~~~~ for record offset info

    protected Map<String, String> tempMap = new HashMap<>();

    protected long count;

    public OffsetLogger() {
    }

    @SuppressWarnings("rawtypes")
    public void logOffset(ConsumerRecord record) {
        count++;

        // 记录offset信息
        String key = record.topic() + "-" + record.partition();
        if (!tempMap.containsKey(key)) {
            ConsumerLog.logFirstOffset(key, record.offset());
        }
        tempMap.put(key, String.valueOf(record.offset()));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void logOffset(ConsumerRecords records) {
        if (records == null || records.isEmpty()) {
            LOG.trace("empty. got 0 records at this poll.");
            return;
        }
        int count = 0;
        Iterable<ConsumerRecord> nrecords = records;
        for (ConsumerRecord record : nrecords) {
            logOffset(record);
            count++;
        }
        LOG.trace("got {} records at this poll.", count);
    }

    public Map<String, String> getOffsetMap() {
        return tempMap;
    }

    public void printOffsetMap(String threadName) {
        if (!tempMap.isEmpty()) {
            Map<String, Long> offsetMap = new HashMap<String, Long>();
            StringBuilder sbu = new StringBuilder();
            for (Map.Entry<String, String> entry : tempMap.entrySet()) {
                String tmp = entry.getValue();
                sbu.append("\r\nOFFSET-INFO:").append(entry.getKey()).append("=").append(tmp);

                offsetMap.put(entry.getKey(), Long.valueOf(tmp));
            }

            ConsumerLog.info("[{}]{}", threadName, sbu.toString());

            ConsumerLog.logLastOffset(offsetMap);
        }

        ConsumerLog.info("[{}]----------total consumer {} records.-----------", threadName, count);
    }

}