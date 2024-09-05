package org.gtpvpair.common;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.gtpvpair.util.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ZkCheckpointUtil implements Serializable {
    //    public static final Flogger L
    public static final String PATH_ZOO_ROOT = "/__consumer_offsets";
    public static final String DEFAULT_OFFSET = "latest";
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkCheckpointUtil.class);

    private transient CountDownLatch connectedSignal = new CountDownLatch(1);

    private String zkUrl;
    private int timeout;
    private transient ZooKeeper zooKeeper;
    public ZkCheckpointUtil(String zkUrl, Integer timeout) throws IOException {
        this.zkUrl = zkUrl;
        if (timeout == null){
            this.timeout = 5000;
        }
        else {
            this.timeout = timeout;

        }
        this.zooKeeper = getConnection();
    };
    public ZooKeeper getConnection() throws IOException{
        Watcher watcher = WatchedEvent -> {
            if (WatchedEvent.getState()== Watcher.Event.KeeperState.SyncConnected){
                if (connectedSignal == null)
                    connectedSignal = new CountDownLatch(1);
                connectedSignal.countDown();
            }
        };
        return new ZooKeeper(zkUrl, timeout, watcher);
    };
    public Map<TopicPartition,Long> convertCheckpoint(String checkpoint){
        Map<TopicPartition,Long> fromOffsets = new HashMap<>();
        if (!DEFAULT_OFFSET.equals(checkpoint)){
            Gson gson = new Gson();
            Map <String, Map<Integer, Long>> offsets = gson.fromJson(checkpoint, new TypeToken<Map<String, Map<Integer,Long>>>(){
            }.getType());
            offsets.forEach((topic,partitionMap)-> partitionMap.forEach((partition, offset)-> fromOffsets.put(new TopicPartition(topic,partition), offset)));
        }
        return fromOffsets;
    };
    public void close(){
        try {
            LOGGER.info("[ZOOKEEPER] Closing zookeeper connection ...");
            zooKeeper.close();
            LOGGER.info("[ZOOKEEPER] Closing zookeeper closed ...");

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean isValid (){return zooKeeper.getState() == ZooKeeper.States.CONNECTING;}
    public void reconnect() throws IOException{
        if (zooKeeper == null){
            zooKeeper = getConnection();
        }
        if (!isValid()) {
            close();
            zooKeeper = getConnection();
        }
    }
    public Stat exists(String path) throws KeeperException, InterruptedException {
        return zooKeeper.exists(path, true);
    }
    public String getData(String path) throws InterruptedException, KeeperException {
        Stat stat = exists(path);
        if (stat != null) {
            try {
                byte[] result = zooKeeper.getData(path, false, null);
                return new String(result, StandardCharsets.UTF_8);
            } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException("Error retrieving data from Zookeeper", e);
            }
        } else {
            return null;
        }
    }

    public String readCheckpoint(String topic, String groupId) throws IOException{
        reconnect();

        String checkpoint = DEFAULT_OFFSET;
        String fullPath = PATH_ZOO_ROOT + "/"  + groupId+"/"+"offset/"+topic;
        try {
            if(exists(fullPath) != null) {
                checkpoint = getData(fullPath);
                LOGGER.warn("Retrieved checkpoint from {} => {}",fullPath, checkpoint);
            } else {
                LOGGER.warn("{} is Null. Checkpoint {} ",fullPath,checkpoint);
            }
        } catch (InterruptedException | KeeperException e) {
            LOGGER.warn("Cannot find checkpoint from {}", fullPath);
            LOGGER.warn(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            close();
        }
        return checkpoint;
    }
    public String determineLastestCheckpoint(String topic, String currentCheckpoint, String inputCheckpoint) {
        Map<TopicPartition, Long> currentOffsets = convertCheckpoint(currentCheckpoint);
        Map<TopicPartition, Long> inputOffsets = convertCheckpoint(inputCheckpoint);
        Map<Integer, Long> offsetMap = new HashMap<>();
        Map<String, Map<Integer,Long>> checkpointMap = new HashMap<>();
        if (currentOffsets.size() != inputOffsets.size() || currentCheckpoint.equals("latest")){
            return inputCheckpoint;
        } else {
            inputOffsets.forEach((tp,offset)->{
                Long currentOffset = currentOffsets.get(tp);
                if (currentOffset > offset)
                    offset = currentOffset;
                offsetMap.put(tp.partition(),offset);
            });
            checkpointMap.put(topic,offsetMap);
            Gson gson = new Gson();
            return gson.toJson(checkpointMap);
        }
    }
    public void create(String path, byte[] data) throws InterruptedException, KeeperException{
        LOGGER.info("CREATE .."+path);
        zooKeeper.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    public void update(String path, byte[] data) throws InterruptedException, KeeperException{
        LOGGER.info("path for zoo"+path);
        zooKeeper.setData(path, data, exists(path).getVersion());
    }
    public  void saveCheckpoint(String topic, String groupId, String checkpoint) throws IOException{
        String currentCheckpoint = readCheckpoint(topic, groupId);
        checkpoint = determineLastestCheckpoint(topic, currentCheckpoint, checkpoint);
        reconnect();
        LOGGER.info("Start save offsets to zookeeper...");
        String fullPath = PATH_ZOO_ROOT;
        try {
            if (exists(fullPath) == null){
                create(fullPath,fullPath.getBytes());
            }
            fullPath += "/" + groupId;
            if (exists(fullPath) == null){
                create(fullPath,fullPath.getBytes());
            }
            fullPath += "/offset";
            if (exists(fullPath) == null){
                create(fullPath,fullPath.getBytes());
            }
            fullPath += "/" + topic;
            if (exists(fullPath) == null){
                create(fullPath,checkpoint.getBytes());
            } else {
                update(fullPath,checkpoint.getBytes());

            }
            LOGGER.info("Save new checkpoint at "+fullPath);
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        } finally {
            close();
        }
    }
    private KafkaConsumer<String, String> createKafkaConsumer() throws IOException {
        Properties props = new Properties();

        ConfigProperty configProperty = ConfigProperty.getInstance();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configProperty.getProperty(ConfigProperty.KAFKA_BROKER) ); // URL của các broker Kafka
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configProperty.getProperty(ConfigProperty.KAFKA_GROUPID_DATAMON)); // group id
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // tắt tự động commit
        return new KafkaConsumer<>(props);
    }
    public Map<TopicPartition, Long> getEarliestOffsets(String topic) throws IOException {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        Map<TopicPartition, Long> earliestOffsets = new HashMap<>();

        try {
            // Get partition information for the topic
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(partitionInfo ->
                    partitions.add(new TopicPartition(topic, partitionInfo.partition()))
            );

            // Assign partitions to the consumer
            consumer.assign(partitions);

            // Seek to beginning and get the earliest offset
            consumer.seekToBeginning(partitions);
            for (TopicPartition partition : partitions) {
                long earliestOffset = consumer.position(partition);
                earliestOffsets.put(partition, earliestOffset);
            }
        } finally {
            consumer.close();
        }

        return earliestOffsets;
    }
    public Map <TopicPartition, Long> validOffset(Map<TopicPartition,Long> checkpointOffset,Map<TopicPartition, Long> earliestOffsets){
        checkpointOffset.forEach((topicPartition, aLong) -> {
            Long earliestOffset = earliestOffsets.get(topicPartition);
            if (aLong < earliestOffset){
                checkpointOffset.put(topicPartition,earliestOffset);
            }
        });
        return checkpointOffset;
    }

}
