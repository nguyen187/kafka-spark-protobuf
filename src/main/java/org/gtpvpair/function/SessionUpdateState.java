package org.gtpvpair.function;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.gtpvpair.message.ExchangeProtoMessage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class SessionUpdateState implements Function2<List<List<ExchangeProtoMessage.ProtMessage>>, Optional<List<ExchangeProtoMessage.ProtMessage>>, Optional<List<ExchangeProtoMessage.ProtMessage>> > {


    public SessionUpdateState() {

    };
    private static Instant parseTimestamp(String timestamp) throws ParseException {
        String DATE_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS";
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        return dateFormat.parse(timestamp).toInstant();
    }
    static boolean isWithinOneMinute(String timestamp, Instant now) {
        try {
            Instant messageTime = parseTimestamp(timestamp);
//            System.out.println("Timestamp: " + Duration.between(messageTime, now).toMinutes());
            return Duration.between(messageTime, now).toMinutes() < 1;
        } catch (ParseException e) {
            // Handle the exception as needed
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Optional<List<ExchangeProtoMessage.ProtMessage>> call(List<List<ExchangeProtoMessage.ProtMessage>> newValues, Optional<List<ExchangeProtoMessage.ProtMessage>> state) throws Exception {
        ExchangeProtoMessage.ProtMessage.Builder messageBuilder = ExchangeProtoMessage.ProtMessage.newBuilder();

        List<ExchangeProtoMessage.ProtMessage> updatedState = state.orElse(new ArrayList<>());
        // Get the current time
        Instant now = Instant.now();

        for (ExchangeProtoMessage.ProtMessage message : updatedState) {
            String imsi = message.getImsi();
            String msisdn = message.getMsisdn();
            messageBuilder.setImsi(imsi);
            messageBuilder.setMsisdn(msisdn);
            if (message.getMessageType().equals("Delete Session Response") && !isWithinOneMinute(updatedState.get(0).getTimestamp(),now))
                return Optional.empty();
            if (!isWithinOneMinute(message.getTimestamp(),now)) {
                return Optional.empty();
            }
        }

        ExchangeProtoMessage.ProtMessage message = new ExchangeProtoMessage.ProtMessage();

        for (List<ExchangeProtoMessage.ProtMessage> value : newValues) {
            for (ExchangeProtoMessage.ProtMessage v : value) {
                messageBuilder.mergeFrom(v);
                message = messageBuilder.build();
                updatedState.add(message);
            }
        }

        return Optional.of(updatedState);
    }
}
