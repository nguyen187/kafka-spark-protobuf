package org.example.common;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
//import java.util.logging.Logger;

public class OffsetRangesManager<K,V> implements Function<JavaRDD<ConsumerRecord<K,V>>, JavaPairRDD<K,V>> {
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
    public JavaPairRDD<K, V> call(JavaRDD<ConsumerRecord<K, V>> rdd) throws Exception {
        OffsetRange[] offests = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        if (offsetMap==null){
            offsetMap = new ConcurrentHashMap<>();
        }
        offsetMap.clear();
        for (OffsetRange offsetRange : offests) {
            offsetMap.put(offsetRange.partition(), offsetRange.untilOffset());
            LOGGER.debug("{}-{}: {}",topic, offsetRange.partition(), offsetRange.untilOffset());

        }
        return rdd.mapToPair(ConsumerRecord -> new Tuple2<>(ConsumerRecord.key(), ConsumerRecord.value()));
    }
    public void commitOffset() throws IOException{
        Gson gson = new Gson();
        Map<String, Map<Integer, Long>> checkpointMap = new ConcurrentHashMap<>();
        checkpointMap.put(topic, offsetMap);
        String checkpoint = gson.toJson(checkpointMap);
        zkCheckpointUtil.saveCheckpoint(topic,groupId,checkpoint);
    }
}
