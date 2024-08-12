package org.example.function;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.example.common.OffsetRangesManager;
import org.example.properties.PublishToKafka;
import org.example.util.ConfigProperty;
import org.json.JSONException;
import org.json.JSONObject;
import scala.collection.Seq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SeqUpdateStateFunc implements Function2<List<String>, Optional<Map<String, String>>, Optional<Map<String, String>>> {
    PublishToKafka kafkaPublisher = PublishToKafka.getInstance();
    private final OffsetRangesManager<String,String> OffsetRanges;
    final ConfigProperty configProperty = ConfigProperty.getInstance();
    final String topicNoPair = configProperty.getProperty(ConfigProperty.KAFKA_TOPIC_NOPAIR);


    public SeqUpdateStateFunc(OffsetRangesManager<String,String> OffsetRanges) throws IOException {
        this.OffsetRanges = OffsetRanges;
    }

    @Override
    public Optional<Map<String, String>> call(List<String> values, Optional<Map<String, String>> state) throws Exception  {
        Map<String, String> combined = new ConcurrentHashMap<>();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Khôi phục trạng thái trước đó nếu có
        if (state != null && state.isPresent()) {
            combined.putAll(state.get());
        }
        String requestTime = null;
        String responseTime = null;
        String seq_id = null;
        try {
            requestTime = combined.getOrDefault("request_time", null); // Lấy từ trạng thái ban đầu
            responseTime = combined.getOrDefault("response_time", null);;  // Lấy từ trạng thái ban đầu
            seq_id = String.valueOf(combined.containsKey("seq_id") ? String.valueOf(combined.get("seq_id")) : null);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        if (responseTime != null )
            return Optional.empty();
        long currentTimeMillis = System.currentTimeMillis();
        // Xóa trạng thái nếu thời gian đã quá 10 giây
        if (requestTime != null) {
            try {
                Date requestDate = dateFormat.parse(requestTime);
                if (currentTimeMillis - requestDate.getTime() > 60000) {
                    JSONObject json = new JSONObject(combined);
                    kafkaPublisher.sendToKafka(this.topicNoPair, seq_id, json);
                    OffsetRanges.commitOffset();
                    return Optional.empty(); // Xóa trạng thái nếu quá 10 giây
                }
            } catch (ParseException e) {
                System.err.println("Có lỗi xảy ra khi phân tích ngày tháng: " + e.getMessage());
            }
        }

        // Duyệt qua từng giá trị trong danh sách values
        for (String value : values) {
            try {
                JSONObject json = new JSONObject(value);
                String timestamp = json.getString("timestamp");

                if (requestTime == null) {
                    requestTime = timestamp;
                    seq_id = json.getString("seq_id");

                }
                else if (timestamp.compareTo(requestTime) > 0) {
                    responseTime = timestamp;
                }


            } catch (JSONException e) {
                // Xử lý ngoại lệ nếu gặp lỗi khi parse JSON
                System.err.println("Có lỗi xảy ra khi phân tích JSON: " + e.getMessage());
            }
        }


        if (requestTime != null) {
            combined.put("seq_id", String.valueOf(seq_id));
            combined.put("request_time", requestTime);


        }
//            combined.put("flag", String.valueOf(0));
        if (responseTime != null) {
            combined.put("response_time", responseTime);
//                combined.put("flag", String.valueOf(1));
        }

        if (seq_id == null || seq_id.isEmpty()) {
            JSONObject json = new JSONObject(combined);
            kafkaPublisher.sendToKafka(this.topicNoPair, String.valueOf(seq_id), json);
            OffsetRanges.commitOffset();
            return Optional.empty();
        }

        return Optional.of(combined);
    }
}