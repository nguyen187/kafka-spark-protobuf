package org.gtpvpair.function;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.gtpvpair.common.OffsetRangesManager;
import org.gtpvpair.message.ExchangeProtoMessage;
import org.gtpvpair.properties.PublishToKafka;
import org.gtpvpair.util.ConfigProperty;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SeqUpdateStateFunc implements Function2<List<ExchangeProtoMessage.ProtMessage>, Optional<List<ExchangeProtoMessage.ProtMessage>>, Optional<List<ExchangeProtoMessage.ProtMessage>>> {
    PublishToKafka kafkaPublisher = PublishToKafka.getInstance();
    private final OffsetRangesManager<String, ExchangeProtoMessage.ProtMessage> OffsetRanges;
    final ConfigProperty configProperty = ConfigProperty.getInstance();
    final String topicNoPair = configProperty.getProperty(ConfigProperty.KAFKA_TOPIC_NOPAIR);


    public SeqUpdateStateFunc(OffsetRangesManager<String, ExchangeProtoMessage.ProtMessage> OffsetRanges) throws IOException {
        this.OffsetRanges = OffsetRanges;
    }

    @Override
    public Optional<List<ExchangeProtoMessage.ProtMessage>> call(List<ExchangeProtoMessage.ProtMessage> newValues, Optional<List<ExchangeProtoMessage.ProtMessage>> state) {
        List<ExchangeProtoMessage.ProtMessage> updatedState = state.orElse(new ArrayList<>());
        if (updatedState.size() == 2)
            return Optional.empty();
        Instant now = Instant.now();
        for (ExchangeProtoMessage.ProtMessage mess : updatedState) {
            if (!SessionUpdateState.isWithinOneMinute(mess.getTimestamp(), now)) {
                return Optional.empty();
            }
        }
        updatedState.addAll(newValues);
        return Optional.of(updatedState);
    };
}
