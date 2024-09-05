package org.gtpvpair.common;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.gtpvpair.message.ExchangeProtoMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import scala.Tuple2;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class OffsetRangesManager<K,V> implements Function<JavaRDD<ConsumerRecord<K, ExchangeProtoMessage.ProtMessage>>, JavaPairRDD<K,V>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetRangesManager.class);
    private final String topic;
    private final String groupId;
    private final ZkCheckpointUtil zkCheckpointUtil;
    private Map<Integer, Long> offsetMap = new ConcurrentHashMap<>();
    public OffsetRangesManager(ZkCheckpointUtil zkCheckpointUtil,String topic, String groupId) {
        this.zkCheckpointUtil = zkCheckpointUtil;
        this.topic = topic;
        this.groupId = groupId;
    }


    @Override
    public JavaPairRDD<K, V> call(JavaRDD<ConsumerRecord<K, ExchangeProtoMessage.ProtMessage>> rdd) throws Exception {
        OffsetRange[] offests = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        if (offsetMap==null){
            offsetMap = new ConcurrentHashMap<>();
        }
        offsetMap.clear();
        for (OffsetRange offsetRange : offests) {
            offsetMap.put(offsetRange.partition(), offsetRange.untilOffset());
            LOGGER.debug("{}-{}: {}",topic, offsetRange.partition(), offsetRange.untilOffset());

        }
        JavaPairRDD<String, ExchangeProtoMessage.ProtMessage> messages = rdd.mapToPair(message ->{
            String key = message.value().getSequenceNumber() + "-" +
                    (message.value().getSourceIp().compareTo(message.value().getDestIp()) < 0 ?
                            message.value().getSourceIp() + "-" + message.value().getDestIp() :
                            message.value().getDestIp() + "-" + message.value().getSourceIp());
            String value = message.value().toString();

            return new Tuple2<>(key, message.value());
        });
        return (JavaPairRDD<K, V>) messages;
    }


    public void commitOffset() throws IOException{
        Gson gson = new Gson();
        Map<String, Map<Integer, Long>> checkpointMap = new ConcurrentHashMap<>();
        checkpointMap.put(topic, offsetMap);
        String checkpoint = gson.toJson(checkpointMap);
        zkCheckpointUtil.saveCheckpoint(topic,groupId,checkpoint);
    }
}
