//package org.example.properties;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.json.JSONObject;
//
//import java.util.Map;
//import java.util.Properties;
//
//public class PublishToKafka {
//
//    // Hàm tạo Kafka Producer
//    public static KafkaProducer<String, String> createProducer() {
//        Map<String, Object> kafkaParams = ProducerProperties.getInstance();
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParams.get("bootstrap.servers"));
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaParams.get("key.serializer"));
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaParams.get("value.serializer"));
//
//        return new KafkaProducer<>(props);
//    }
//
//    // Hàm gửi dữ liệu lên Kafka
//    public static void sendToKafka(String topic, String key, JSONObject json) {
//        try (KafkaProducer<String, String> producer = createProducer()) {
//            String valueJson = json.toString();
//            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, valueJson);
//
//            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
//                if (exception != null) {
//                    exception.printStackTrace();
//                } else {
////                    System.out.println("Sent record to partition " + metadata.partition() +
////                            " with offset " + metadata.offset());
//                }
//            });
//        }
//    }
//}
package org.example.properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

public class PublishToKafka implements Serializable {

    private static PublishToKafka instance;
    private static KafkaProducer<String, String> producer;

    // Private constructor to prevent instantiation
    private PublishToKafka() {
        // Initialize Kafka producer
        Map<String, Object> kafkaParams = ProducerProperties.getInstance();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaParams.get("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaParams.get("key.serializer"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaParams.get("value.serializer"));

        producer = new KafkaProducer<>(props);
    }

    // Public method to provide access to the singleton instance
    public static synchronized PublishToKafka getInstance() {
        if (instance == null) {
            instance = new PublishToKafka();
        }
        return instance;
    }

    // Method to send data to Kafka
    public void sendToKafka(String topic, String key, JSONObject json) {
        if (producer == null) {
            throw new IllegalStateException("KafkaProducer is not initialized");
        }

        String valueJson = json.toString();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, valueJson);

        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            }

        });
    }
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }


}
