package org.example.properties;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerProperties {
    /**
     * Define configuration for Kafka producer
     * @return Map<String, Object> with Kafka producer configurations
     */
    public static Map<String, Object> getInstance() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.117.131:9092");
        kafkaParams.put("key.serializer", StringSerializer.class.getName());
        kafkaParams.put("value.serializer", StringSerializer.class.getName());
        kafkaParams.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaParams.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaParams.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        return kafkaParams;
    }
}

