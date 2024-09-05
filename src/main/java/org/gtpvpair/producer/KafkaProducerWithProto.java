package org.gtpvpair.producer;


import org.gtpvpair.message.ExchangeProtoMessage.ProtMessage;
import org.gtpvpair.model.ProtMessageSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerWithProto {

    public static void main(String[] args) {

        System.out.println("going to publish messages");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");



        Producer<Integer, ProtMessage> producer = new KafkaProducer<>(props, new IntegerSerializer(), new ProtMessageSerializer());
        String filePath = "src/DATA/gtpv_match.csv"; // Thay đổi đường dẫn đến tệp CSV của bạn

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            // Đọc từng dòng từ tệp CSV
            while ((line = br.readLine()) != null) {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
                LocalDateTime now = LocalDateTime.now();
                if (line.startsWith("\uFEFF")) {
                    line = line.substring(1);
                }
                String[] pairs = line.split(",\\s*");

                Map<String, String> dataMap = new HashMap<>();

                for (String pair : pairs) {
                    // Phân tách cặp khóa-giá trị bằng dấu '='
                    String[] keyValue = pair.split("=", 2); // Chỉ phân tách thành 2 phần để giữ giá trị có dấu '='
                    if (keyValue.length == 2) {
                        // Thêm cặp khóa-giá trị vào Map
                        dataMap.put(keyValue[0].trim(), keyValue[1].trim());
                    }
                }
// Xây dựng đối tượng ProtMessage từ Map

                ProtMessage.Builder messageBuilder = ProtMessage.newBuilder()

                        .setTimestamp(String.valueOf(dtf.format(now)))
                        .setSourceIp(dataMap.get("source-ip"))
                        .setDestIp(dataMap.get("dest-ip"))
                        .setMessageType(dataMap.get("message-type"))
                        .setSequenceNumber(Integer.parseInt(dataMap.get("sequence-number")));

                // Set các trường optional nếu có

                if (dataMap.containsKey("imsi")) {
                    messageBuilder.setImsi(dataMap.get("imsi"));
                }
                if (dataMap.containsKey("msisdn")) {
                    messageBuilder.setMsisdn(dataMap.get("msisdn"));
                }
                if (dataMap.containsKey("teid")) {
                    messageBuilder.setTeid(dataMap.get("teid"));
                }
                if (dataMap.containsKey("cause")) {
                    messageBuilder.setCause(dataMap.get("cause"));
                }
                if (dataMap.containsKey("user-location")) {
                    messageBuilder.setUserLocation(dataMap.get("user-location"));
                }
                if (dataMap.containsKey("bearer-context")) {
                    messageBuilder.setBearerContext(dataMap.get("bearer-context"));
                }
                if (dataMap.containsKey("dpi_ip")) {
                    messageBuilder.setDpiIp(dataMap.get("dpi_ip"));
                }

                ProtMessage message = messageBuilder.build();

                // Gửi tin nhắn lên Kafka
                System.out.println(message);
                producer.send(new ProducerRecord<>("datamon",0,Integer.valueOf(dataMap.get("sequence-number")), message));
                TimeUnit.SECONDS.sleep(2);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        producer.close();
    }
}
