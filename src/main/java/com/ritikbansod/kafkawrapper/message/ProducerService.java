package com.ritikbansod.kafkawrapper.message;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Produces records through the connection's raw byte producer.
 * Supports optional partition pinning, custom timestamp and headers.
 * Values may arrive as UTF-8 text or base64-encoded binary (schema payloads).
 */
@Service
public class ProducerService {

    public Map<String, Object> produce(ClusterHandle handle, String topic, String key, String value,
                                       String valueBase64, Integer partition, Long timestamp,
                                       Map<String, String> headers)
            throws ExecutionException, InterruptedException, TimeoutException {
        byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = valueBase64 != null && !valueBase64.isBlank()
                ? java.util.Base64.getDecoder().decode(valueBase64)
                : (value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        if (valueBytes == null) {
            throw new IllegalArgumentException("Either 'payload' or 'payloadBase64' is required");
        }

        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, partition, timestamp, keyBytes, valueBytes);
        if (headers != null) {
            headers.forEach((name, headerValue) -> {
                if (headerValue != null) {
                    record.headers().add(name, headerValue.getBytes(StandardCharsets.UTF_8));
                }
            });
        }

        RecordMetadata metadata = handle.template().send(record)
                .get(15, TimeUnit.SECONDS)
                .getRecordMetadata();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topic", metadata.topic());
        result.put("partition", metadata.partition());
        result.put("offset", metadata.offset());
        result.put("timestamp", metadata.timestamp());
        if (key != null) {
            result.put("key", key);
        }
        return result;
    }
}

