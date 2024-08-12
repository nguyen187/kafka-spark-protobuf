package org.example.util;

import org.apache.spark.SparkConf;

import java.io.*;
import java.util.Properties; // Import lớp Properties để xử lý file config

public class ConfigProperty implements Serializable {

    private static final Properties properties = new Properties(); // Tạo đối tượng Properties để lưu cấu hình
    private static final String configFilePath = "src/main/resources/kafka.properties"; // Đường dẫn đến file config
    private static final String configFilePathSpark = "src/main/resources/spark.properties"; // Đường dẫn đến file config
    public static final String JOBNAME = "jobname" ;//= 192.168.117:2128

    private static ConfigProperty instance; // Singleton instance
    public static final String ZOOKEEPER_QUORUM = "config.zookeeper.quorum" ;//= 192.168.117:2128
    public static final String KAFKA_TOPIC_PAIR = "n1.topic.pair" ;//= 192.168.117:2128
    public static final String MICROBATCH_DURATION = "microbatch.duration";
    public static final String NUM_BATCH_CHECKPOINT = "num.batch.checkpoint";
    public static final String KAFKA_TOPIC_NOPAIR = "n1.topic.nopair";
    public static final String CHECKPOINT_PATH_SPARK = "checkpoint.path.spark";
    public static final String TIMOEOUT_CONNECT_ZK = "timeout.connect.zk";
    public static final String KAFKA_GROUPID_DATAMON = "groupt.id.datamon";
    public static final String KAFKA_TOPIC_INPUT = "n1.topic.input";

    // Private constructor để ngăn việc khởi tạo từ bên ngoài
    private ConfigProperty() throws IOException {
        load(configFilePath); // Nạp cấu hình từ file khi khởi tạo
    }

    // Phương thức để lấy instance duy nhất của ConfigProperty
    public static ConfigProperty getInstance() throws IOException {
        if (instance == null) {
            synchronized (ConfigProperty.class) {
                if (instance == null) {
                    instance = new ConfigProperty(); // Khởi tạo instance nếu chưa tồn tại
                }
            }
        }
        return instance;
    }

    // Phương thức nạp cấu hình từ file
    private Properties load(String path) throws IOException {
        try (InputStream inputStream = new FileInputStream(path)) {
            properties.load(inputStream); // Nạp các thuộc tính từ file config
        }
        return properties;
    }

    // Phương thức để lấy giá trị thuộc tính theo key
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    // Phương thức để lấy giá trị thuộc tính theo key với giá trị mặc định
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    // Phương thức để lấy giá trị integer từ thuộc tính theo key
    public int getIntProperty(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    // Phương thức để lấy tất cả các thuộc tính
    public Properties getAllProperties() {
        return properties;
    }
    public SparkConf loadSparkConf() throws IOException {
        SparkConf conf = new SparkConf();
        Properties props = load(configFilePathSpark);
        // Cấu hình Spark từ file properties
        assert props != null;
        conf.set("spark.executor.memory", props.getProperty("spark.executor.memory"));
        conf.set("spark.driver.memory", props.getProperty("spark.driver.memory"));
        conf.set("spark.executor.cores", props.getProperty("spark.executor.cores"));
        conf.set("spark.driver.cores", props.getProperty("spark.driver.cores"));
        conf.set("spark.default.parallelism", props.getProperty("spark.default.parallelism"));
        conf.set("spark.sql.shuffle.partitions", props.getProperty("spark.sql.shuffle.partitions"));
        conf.set("spark.streaming.backpressure.enabled", props.getProperty("spark.streaming.backpressure.enabled"));
        conf.set("spark.streaming.unpersist", props.getProperty("spark.streaming.unpersist", "true"));
        conf.set("spark.streaming.kafka.maxRatePerPartition", props.getProperty("spark.streaming.kafka.maxRatePerPartition"));
        conf.set("spark.streaming.kafka.consumer.poll.ms", props.getProperty("spark.streaming.kafka.consumer.poll.ms"));
        conf.set("spark.sql.autoBroadcastJoinThreshold", props.getProperty("spark.sql.autoBroadcastJoinThreshold"));
        conf.set("spark.eventLog.enabled", props.getProperty("spark.eventLog.enabled"));
        conf.set("spark.eventLog.dir", props.getProperty("spark.eventLog.dir"));
        conf.set("spark.history.fs.logDirectory", props.getProperty("spark.history.fs.logDirectory"));

        return conf;
    }
}
