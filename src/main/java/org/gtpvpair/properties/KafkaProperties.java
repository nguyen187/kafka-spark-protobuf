package org.gtpvpair.properties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gtpvpair.model.ProtMessageDeserializer;
import org.gtpvpair.util.ConfigProperty;

public class KafkaProperties {

    /**
     * Define configuration kafka consumer with spark streaming
     * @return
     */

    public static Map<String, Object> getInstance() throws IOException {
        ConfigProperty configProperty = ConfigProperty.getInstance();

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", configProperty.getProperty(ConfigProperty.KAFKA_BROKER));//"192.168.117.131:9092"
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", configProperty.getProperty(ConfigProperty.KAFKA_GROUPID_DATAMON));
//        kafkaParams.put("auto.offset.reset", "none");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtMessageDeserializer.class);
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("receive.buffer.bytes", 102400);

        return kafkaParams;
    }
}