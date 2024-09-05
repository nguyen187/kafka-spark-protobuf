package org.gtpvpair.properties;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gtpvpair.util.ConfigProperty;

public class ProducerProperties {
    private final String kfBroker;

    /**
     * Define configuration for Kafka producer
     * @return Map<String, Object> with Kafka producer configurations
     */



    public ProducerProperties() throws IOException {
        final ConfigProperty configProperty = ConfigProperty.getInstance();
        this.kfBroker = configProperty.getProperty(ConfigProperty.KAFKA_BROKER);

    }


    public static Map<String, Object> getInstance() throws IOException {

        Map<String, Object> kafkaParams = new ConcurrentHashMap<>();
        kafkaParams.put("bootstrap.servers", ConfigProperty.getInstance().getProperty(ConfigProperty.KAFKA_BROKER));
        kafkaParams.put("key.serializer", StringSerializer.class.getName());
        kafkaParams.put("value.serializer", StringSerializer.class.getName());
        kafkaParams.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaParams.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaParams.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        return kafkaParams;
    }
}

