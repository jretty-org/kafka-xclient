package org.jretty.kafka.common;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleOffsetLogger {
    
    private static final Logger LOG = LoggerFactory.getLogger(SimpleOffsetLogger.class);

    // ~~~~~~~~~~~~~ for record offset info
    protected Map<String, Long> tempMap = new HashMap<>();
    
    protected long count;

    public void logOffset(String topic, int partition, long offset) {
        count++;
        // 记录offset信息
        String key = topic + "-" + partition;
        if (!tempMap.containsKey(key)) {
            LOG.info("\r\nOFFSET-INFO:{}={}", key, offset);
        }
        tempMap.put(key, offset);
    }

    public Map<String, Long> getOffsetMap() {
        return tempMap;
    }

    public void printOffsetMap(String name) {
        if (!tempMap.isEmpty()) {
            StringBuilder sbu = new StringBuilder();
            for (Map.Entry<String, Long> entry : tempMap.entrySet()) {
                sbu.append("\r\nOFFSET-INFO:").append(entry.getKey()).append("=").append(entry.getValue());
            }
            LOG.info("[{}]{}", name, sbu.toString());
        }
        LOG.info("[{}]----------Total recieve {} records.-----------", name, count);
    }
}
