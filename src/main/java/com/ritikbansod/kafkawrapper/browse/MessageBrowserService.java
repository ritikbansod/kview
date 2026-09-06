package com.ritikbansod.kafkawrapper.browse;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import com.ritikbansod.kafkawrapper.schema.SchemaRegistryService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Read-only message browser: assigns partitions, seeks, polls up to a limit.
 * Never commits offsets and never joins a real consumer group, so browsing
 * does not interfere with running applications.
 */
@Service
public class MessageBrowserService {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);

    private final SchemaRegistryService registryService;

    public MessageBrowserService(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    public BrowseResult browse(ClusterHandle handle, String topic, BrowseRequest request) {
        int limit = request.limit() == null ? 50 : Math.min(Math.max(request.limit(), 1), 1000);
        long timeoutMs = request.timeoutMs() == null ? 5_000 : Math.min(Math.max(request.timeoutMs(), 500), 60_000);

        Map<String, Object> props = new HashMap<>(handle.consumerProperties());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kview-browser-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.min(limit, 500));

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = resolvePartitions(consumer, topic, request.partition());
            // Explicit offsets imply "browse exactly these partitions"
            if ("offsets".equals(request.startOrDefault()) && request.offsets() != null && !request.offsets().isEmpty()) {
                partitions = partitions.stream()
                        .filter(tp -> request.offsets().containsKey(tp.partition()))
                        .toList();
            }
            if (partitions.isEmpty()) {
                throw new org.apache.kafka.common.errors.UnknownTopicOrPartitionException(topic);
            }
            consumer.assign(partitions);

            switch (request.startOrDefault()) {
                case "earliest" -> consumer.seekToBeginning(partitions);
                case "latest" -> seekLatestBackwards(consumer, partitions, limit);
                case "offsets" -> {
                    Map<TopicPartition, Long> targets = new HashMap<>();
                    partitions.forEach(tp -> {
                        Long offset = request.offsets() == null ? null : request.offsets().get(tp.partition());
                        if (offset != null) {
                            targets.put(tp, offset);
                        }
                    });
                    consumer.seekToBeginning(partitions);
                    targets.forEach(consumer::seek);
                }
                case "timestamp" -> {
                    long ts = request.timestamp() == null ? 0 : request.timestamp();
                    Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> found =
                            consumer.offsetsForTimes(partitions.stream()
                                    .collect(HashMap::new, (m, tp) -> m.put(tp, ts), HashMap::putAll));
                    List<TopicPartition> unmatched = new ArrayList<>();
                    for (TopicPartition tp : partitions) {
                        OffsetAndTimestamp at = found.get(tp);
                        if (at != null) {
                            consumer.seek(tp, at.offset());
                        } else {
                            unmatched.add(tp);
                        }
                    }
                    if (!unmatched.isEmpty()) {
                        consumer.seekToEnd(unmatched); // nothing at/after ts: return nothing from these
                    }
                }
                default -> throw new IllegalArgumentException(
                        "Unknown start mode '" + request.start() + "' (use earliest, latest, offsets or timestamp)");
            }

            long deadline = System.currentTimeMillis() + timeoutMs;
            List<ConsumerRecord<byte[], byte[]>> collected = new ArrayList<>();
            String keyContains = lower(request.keyContains());
            String valueContains = lower(request.valueContains());

            while (collected.size() < limit && System.currentTimeMillis() < deadline) {
                var records = consumer.poll(POLL_INTERVAL);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (keyContains != null && lower(asText(record.key())) != null
                            && !lower(asText(record.key())).contains(keyContains)) {
                        continue;
                    }
                    if (valueContains != null && lower(asText(record.value())) != null
                            && !lower(asText(record.value())).contains(valueContains)) {
                        continue;
                    }
                    collected.add(record);
                    if (collected.size() >= limit) {
                        break;
                    }
                }
            }
            collected.sort(Comparator
                    .comparingInt((ConsumerRecord<byte[], byte[]> r) -> r.partition())
                    .thenComparingLong(ConsumerRecord::offset));

            String clusterId = handle.profile().id();
            List<BrowserMessage> messages = collected.stream()
                    .map(r -> toView(clusterId, r))
                    .toList();
            return new BrowseResult(topic, partitions.stream().map(TopicPartition::partition).sorted().toList(),
                    messages, messages.size() < limit);
        }
    }

    /** For "latest" we back off from the end offset so the last N messages are visible immediately. */
    private void seekLatestBackwards(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions, int limit) {
        Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
        Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(partitions);
        int perPartition = Math.max(1, limit / Math.max(1, partitions.size()));
        ends.forEach((tp, end) -> {
            long beginning = beginnings.getOrDefault(tp, 0L);
            consumer.seek(tp, Math.max(beginning, end - perPartition));
        });
    }

    private List<TopicPartition> resolvePartitions(KafkaConsumer<byte[], byte[]> consumer, String topic,
                                                   Integer onlyPartition) {
        List<TopicPartition> result = new ArrayList<>();
        var partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(10));
        if (partitionInfos == null) {
            return result;
        }
        partitionInfos.forEach(info -> {
            if (onlyPartition == null || onlyPartition == info.partition()) {
                result.add(new TopicPartition(topic, info.partition()));
            }
        });
        return result;
    }

    private BrowserMessage toView(String clusterId, ConsumerRecord<byte[], byte[]> record) {
        byte[] keyBytes = record.key();
        byte[] valueBytes = record.value();
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            headers.putIfAbsent(header.key(),
                    header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8));
        }
        // schema-registry decode (no-op returning UNKNOWN when no registry is attached)
        SchemaRegistryService.DecodedPayload schema =
                registryService.decode(clusterId, valueBytes, record.topic(), false);
        return new BrowserMessage(record.topic(), record.partition(), record.offset(),
                record.timestamp(), record.timestampType() == null ? null : record.timestampType().name(),
                asText(keyBytes), asText(valueBytes), headers,
                keyBytes == null ? null : Base64.getEncoder().encodeToString(keyBytes),
                valueBytes == null ? null : Base64.getEncoder().encodeToString(valueBytes),
                isStrictUtf8(keyBytes) && isStrictUtf8(valueBytes),
                schema);
    }

    static String asText(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    static boolean isStrictUtf8(byte[] bytes) {
        if (bytes == null) return true;
        try {
            StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String lower(String s) {
        return s == null || s.isBlank() ? null : s.toLowerCase();
    }

    public record BrowseRequest(Integer partition,
                                String start,
                                Map<Integer, Long> offsets,
                                Long timestamp,
                                Integer limit,
                                Long timeoutMs,
                                String keyContains,
                                String valueContains) {

        public String startOrDefault() {
            return start == null || start.isBlank() ? "earliest" : start;
        }
    }

    public record BrowserMessage(String topic, int partition, long offset, long timestamp, String timestampType,
                                 String key, String value, Map<String, String> headers,
                                 String keyBase64, String valueBase64, boolean binary,
                                 SchemaRegistryService.DecodedPayload schema) {
    }

    public record BrowseResult(String topic, List<Integer> partitions,
                               List<BrowserMessage> messages, boolean reachedEnd) {
    }
}
