package org.gtpvpair.main;

import java.io.IOException;
import java.util.*;

import com.google.gson.Gson;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.gtpvpair.common.CompositeKey;
import org.gtpvpair.function.SessionUpdateState;
import org.gtpvpair.message.ExchangeProtoMessage;
import org.gtpvpair.properties.KafkaProperties;
import org.gtpvpair.properties.PublishToKafka;
import org.gtpvpair.common.OffsetRangesManager;
import org.gtpvpair.common.ZkCheckpointUtil;
import org.gtpvpair.function.SeqUpdateStateFunc;
import org.json.JSONObject;
import org.gtpvpair.util.ConfigProperty;
import scala.Tuple2;
import org.gtpvpair.util.GenerateUniquekey;
public class StreamingApp {
    public static void main(String[] args) throws InterruptedException, IOException {
        ConfigProperty configProperty = ConfigProperty.getInstance();
        String jobName = configProperty.getProperty(ConfigProperty.JOBNAME);//"DM-212";

//        SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark Streaming Consumer");
        SparkConf conf = configProperty.loadSparkConf().setMaster("local").setAppName(jobName);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        // Define a list of Kafka topic to subscribe
        JavaPairDStream<String, ExchangeProtoMessage.ProtMessage> stream;

        String groupId = configProperty.getProperty(ConfigProperty.KAFKA_GROUPID_DATAMON);//"DM-212";
        String topicPair = configProperty.getProperty(ConfigProperty.KAFKA_TOPIC_PAIR);//"datamon";
        String topicsNoPair = configProperty.getProperty(ConfigProperty.KAFKA_TOPIC_NOPAIR);//"datamon";
        String topicInput = configProperty.getProperty(ConfigProperty.KAFKA_TOPIC_INPUT);//"datamon";

        final String zookeeper = configProperty.getProperty(ConfigProperty.ZOOKEEPER_QUORUM);//"192.168.117.131:2181";
        final int timeoutConnectZk = configProperty.getIntProperty(ConfigProperty.TIMOEOUT_CONNECT_ZK);//5000;
        final int microbatchDuration = configProperty.getIntProperty(ConfigProperty.MICROBATCH_DURATION);//1000;
        final int numBatchCheckpoint = configProperty.getIntProperty(ConfigProperty.NUM_BATCH_CHECKPOINT);//10;
        final String checkpointSpark = configProperty.getProperty(ConfigProperty.CHECKPOINT_PATH_SPARK);//"192.168.117.131:2181";
        final String listBroker = configProperty.getProperty(ConfigProperty.KAFKA_BROKER);
        System.out.println("*********************************************************");
        System.out.println("Kafka Group ID: " + groupId);
        System.out.println("Kafka Topics (Pair): " + topicInput);
        System.out.println("Kafka Topics (Pair): " + topicPair);
        System.out.println("Kafka Topics (No Pair): " + topicsNoPair);
        System.out.println("Zookeeper Quorum: " + zookeeper);
        System.out.println("Timeout Connect ZK: " + timeoutConnectZk);
        System.out.println("Microbatch Duration: " + microbatchDuration);
        System.out.println("Number of Batch Checkpoints: " + numBatchCheckpoint);
        System.out.println("Checkpoint Path for Spark: " + checkpointSpark);
        System.out.println("*********************************************************");

        // Lấy giá trị của các thuộc tính từ file config

        ZkCheckpointUtil zoocheckpointUtil = new ZkCheckpointUtil(zookeeper,timeoutConnectZk);
        Duration checkPointDuration = Durations.milliseconds((long) numBatchCheckpoint * microbatchDuration);
        final OffsetRangesManager<String, ExchangeProtoMessage.ProtMessage> OffsetRanges = new OffsetRangesManager<>(zoocheckpointUtil,topicInput,groupId);
        PublishToKafka kafkaPublisher = PublishToKafka.getInstance();

        String checkpoint = zoocheckpointUtil.readCheckpoint(topicInput,groupId);
        Map<TopicPartition, Long> checkpointOffsets = zoocheckpointUtil.convertCheckpoint(checkpoint);
        Map<TopicPartition, Long> earliestOffsets = zoocheckpointUtil.getEarliestOffsets(topicInput);
        Map<TopicPartition, Long> fromOffsets = zoocheckpointUtil.validOffset(checkpointOffsets, earliestOffsets);

        System.out.println("Start offsets "+fromOffsets);
        Collection<String> Kftopics = Collections.singletonList(topicInput);

        stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, ExchangeProtoMessage.ProtMessage>Subscribe(Kftopics, KafkaProperties.getInstance(), fromOffsets)
        ).transformToPair(OffsetRanges);

        jssc.checkpoint(checkpointSpark);

        JavaPairDStream<String, List<ExchangeProtoMessage.ProtMessage>> reqRespPair = stream.updateStateByKey(new SeqUpdateStateFunc(OffsetRanges));
        reqRespPair.checkpoint(checkPointDuration);

        JavaPairDStream<String, List<ExchangeProtoMessage.ProtMessage>> sessReqRespPair = reqRespPair.mapToPair(r->{
            String teid = null;
            if (r._2.size() < 2)
                return null;
            for (ExchangeProtoMessage.ProtMessage protMessage : r._2) {
                if (protMessage.getMessageType().contains("Response")) {
                    teid = protMessage.getTeid();
                    break;
                }
            }
            return new Tuple2<>(teid,r._2);
        }).filter(Objects::nonNull);

        JavaPairDStream<String, List<ExchangeProtoMessage.ProtMessage>> sessionResqRespEnrich = sessReqRespPair.updateStateByKey(new SessionUpdateState());
        sessionResqRespEnrich.checkpoint(checkPointDuration);



        sessionResqRespEnrich.foreachRDD(rdd -> {
            Set<String> sentMessages = new HashSet<>();
            System.out.println("==============================");
            System.out.println(sentMessages);
            rdd.foreach(tuple -> {

                List<ExchangeProtoMessage.ProtMessage> protMessages = tuple._2(); // Accessing the list of ProtMessage
                Iterator<ExchangeProtoMessage.ProtMessage> iterator = protMessages.iterator();
                while (iterator.hasNext()) {
                    ExchangeProtoMessage.ProtMessage message1 = iterator.next();
                    ExchangeProtoMessage.ProtMessage message2 = iterator.hasNext() ? iterator.next() : null;

                    String key = GenerateUniquekey.run(message1);
                    System.out.println("+++++++++++++++++++++++++++++++++");
                    System.out.println(sentMessages);
                    if (!sentMessages.contains(key)) {
                        String concatenatedMessage;
                        assert message2 != null;
                        concatenatedMessage = message1 + "\n" + message2;
                        System.out.println("-----------------------------------");
                        System.out.println(concatenatedMessage);
                        kafkaPublisher.sendToKafka(topicPair, key, concatenatedMessage);
                        OffsetRanges.commitOffset();
                        sentMessages.add(key);
                        System.out.println(key);
                    }
                }
            });
        });

//            rdd.foreach(p->{
//                Iterator<ExchangeProtoMessage.ProtMessage> iterator = p.iterator();
//                while (iterator.hasNext()) {
//                    System.out.println("PPPPPPPPPPPPPPPPPPPPPPP");
//
//                    String item = iterator.next();
//                    System.out.println(item);
//                }

////                            JSONObject json = new JSONObject(valueMap);
//                            kafkaPublisher.sendToKafka(topicPair,,json);
//                            OffsetRanges.commitOffset();
//            });
//        });
//        sessionResqRespEnrich.print();

//        updatedStream.print();
//        updatedStream.foreachRDD(rdd -> {
//            rdd.foreach(partition -> {
//                    Map<String, String> valueMap = partition._2; // Value của cặp (key, valu
//                    if (partition._2().containsKey("response_time")) {
//                        try {
//                            String key = partition._1; // Key của cặp (key, value)
//                            JSONObject json = new JSONObject(valueMap);
//                            kafkaPublisher.sendToKafka(topicPair,valueMap.get("seq_id"),json);
//                            OffsetRanges.commitOffset();
//                            System.out.println(json);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//
//                    }
//            });
//
//        });
//        updatedStream.print();
//        updatedStream.checkpoint(checkPointDuration);

        jssc.start();
        jssc.awaitTermination();
    }

}

