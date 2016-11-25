package org.jretty.kafka.common;

import java.util.HashMap;
import java.util.Map;

public class CommonKafkaConfig {

    private final Map<String, Object> configs;

    public CommonKafkaConfig(Map<?, ?> configs) {
        Map<String, Object> map = new HashMap<>(configs.size());
        for(Map.Entry<?, ?> entry: configs.entrySet()) {
            map.put((String)entry.getKey(), entry.getValue());
        }
        this.configs = map;
    }

    public Map<String, Object> getConfigs() {
        return configs;
    }

    public String getBrokerUrl() {
        return (String) configs.get("bootstrap.servers");
    }

}