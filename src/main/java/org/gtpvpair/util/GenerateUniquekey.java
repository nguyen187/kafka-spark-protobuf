package org.gtpvpair.util;

import org.gtpvpair.message.ExchangeProtoMessage;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class GenerateUniquekey {

    public static String run(ExchangeProtoMessage.ProtMessage message) {
        try {
            // Combine important fields into a single string
            String rawData = message.getSequenceNumber() + message.getSourceIp() + message.getDestIp();

            // Use a hash function (e.g., SHA-256) to generate a unique key
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(rawData.getBytes(StandardCharsets.UTF_8));

            // Convert the hash to a base64 string (or hex if preferred)
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error generating unique key", e);
        }
    }
}
