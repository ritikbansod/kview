package com.ritikbansod.kafkawrapper.live;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import com.ritikbansod.kafkawrapper.schema.SchemaRegistryService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live message tailing over Server-Sent Events. Each tail runs a dedicated
 * consumer thread; by default it does not join a group and does not commit,
 * so it is a pure read-only peek. Optionally it can run as a real group member
 * (groupId + autoCommit) to visualize what an application consumer receives.
 */
@Service
public class LiveTailService {

    public static final int MAX_ACTIVE_TAILS = 25;

    private static final Logger log = LoggerFactory.getLogger(LiveTailService.class);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(400);

    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "kview-tail");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<String, TailSession> sessions = new ConcurrentHashMap<>();
    private final AtomicLong tailCounter = new AtomicLong();
    private final SchemaRegistryService registryService;

    public LiveTailService(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    public synchronized TailSession start(ClusterHandle handle, String topic, TailRequest request) {
        if (sessions.size() >= MAX_ACTIVE_TAILS) {
            throw new IllegalStateException("Too many active tails (" + MAX_ACTIVE_TAILS + ") — stop one first");
        }
        String tailId = "tail-" + tailCounter.incrementAndGet() + "-" + UUID.randomUUID().toString().substring(0, 8);
        SseEmitter emitter = new SseEmitter(0L); // no server-side timeout; client disconnect ends it
        TailSession session = new TailSession(tailId, topic, emitter);
        sessions.put(tailId, session);

        Runnable cleanup = () -> {
            sessions.remove(tailId);
            session.stop();
        };
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(e -> cleanup.run());

        executor.submit(() -> run(session, handle, request));
        return session;
    }

    public boolean stop(String tailId) {
        TailSession session = sessions.get(tailId);
        if (session == null) {
            return false;
        }
        session.stop();
        try {
            session.emitter().complete();
        } catch (RuntimeException ignored) {
            // already completed by client disconnect
        }
        sessions.remove(tailId);
        return true;
    }

    public List<TailInfo> active() {
        return sessions.values().stream()
                .map(s -> new TailInfo(s.tailId(), s.topic()))
                .toList();
    }

    private void run(TailSession session, ClusterHandle handle, TailRequest request) {
        Map<String, Object> props = new java.util.HashMap<>(handle.consumerProperties());
        String groupId = request.groupId();
        boolean joinGroup = groupId != null && !groupId.isBlank();
        boolean autoCommit = joinGroup && Boolean.TRUE.equals(request.autoCommit());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                joinGroup ? groupId : "kview-tail-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest".equalsIgnoreCase(request.fromOrDefault()) ? "earliest" : "latest");

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            session.attach(consumer);
            if (joinGroup && autoCommit) {
                consumer.subscribe(List.of(session.topic()));
            } else {
                List<TopicPartition> partitions = new ArrayList<>();
                consumer.partitionsFor(session.topic(), Duration.ofSeconds(10))
                        .forEach(info -> {
                            if (request.partition() == null || request.partition() == info.partition()) {
                                partitions.add(new TopicPartition(session.topic(), info.partition()));
                            }
                        });
                consumer.assign(partitions);
                if ("earliest".equalsIgnoreCase(request.fromOrDefault())) {
                    consumer.seekToBeginning(partitions);
                } else {
                    consumer.seekToEnd(partitions);
                }
            }

            send(session, "open", Map.of("tailId", session.tailId(), "topic", session.topic()));

            while (session.running() && !Thread.currentThread().isInterrupted()) {
                var records = session.consumer().poll(POLL_INTERVAL);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (!session.running() || !send(session, "message", toMap(handle.profile().id(), record))) {
                        break;
                    }
                }
                if (autoCommit) {
                    try {
                        consumer.commitAsync();
                    } catch (RuntimeException e) {
                        log.debug("Tail {} commit failed: {}", session.tailId(), e.getMessage());
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException expected) {
            // stopping the session wakes the poll loop
        } catch (Exception e) {
            log.info("Tail {} failed: {}", session.tailId(), e.getMessage());
            send(session, "error", Map.of("message", String.valueOf(e.getMessage())));
        } finally {
            session.stop();
            sessions.remove(session.tailId());
            try {
                session.emitter().complete();
            } catch (RuntimeException ignored) {
                // already completed by client disconnect
            }
        }
    }

    private boolean send(TailSession session, String event, Object payload) {
        try {
            synchronized (session) {
                session.emitter().send(SseEmitter.event().name(event)
                        .data(payload, MediaType.APPLICATION_JSON));
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Map<String, Object> toMap(String clusterId, ConsumerRecord<byte[], byte[]> record) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("topic", record.topic());
        view.put("partition", record.partition());
        view.put("offset", record.offset());
        view.put("timestamp", record.timestamp());
        byte[] keyBytes = record.key();
        byte[] valueBytes = record.value();
        view.put("key", keyBytes == null ? null : new String(keyBytes, StandardCharsets.UTF_8));
        view.put("value", valueBytes == null ? null : new String(valueBytes, StandardCharsets.UTF_8));
        view.put("valueBase64", valueBytes == null ? null : Base64.getEncoder().encodeToString(valueBytes));
        view.put("binary", valueBytes != null && !isStrictUtf8(valueBytes));
        view.put("schema", registryService.decode(clusterId, valueBytes, record.topic(), false));
        Map<String, String> headers = new LinkedHashMap<>();
        record.headers().forEach(h -> headers.put(h.key(),
                h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8)));
        view.put("headers", headers);
        return view;
    }

    static boolean isStrictUtf8(byte[] bytes) {
        if (bytes == null) return true;
        try {
            java.nio.charset.StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public record TailRequest(String from, Integer partition, String groupId, Boolean autoCommit) {

        public String fromOrDefault() {
            return from == null || from.isBlank() ? "latest" : from;
        }
    }

    public record TailInfo(String tailId, String topic) {
    }

    public static final class TailSession {
        private final String tailId;
        private final String topic;
        private final SseEmitter emitter;
        private volatile KafkaConsumer<byte[], byte[]> consumer;
        private volatile boolean running = true;

        TailSession(String tailId, String topic, SseEmitter emitter) {
            this.tailId = tailId;
            this.topic = topic;
            this.emitter = emitter;
        }

        public String tailId() {
            return tailId;
        }

        public String topic() {
            return topic;
        }

        public SseEmitter emitter() {
            return emitter;
        }

        void attach(KafkaConsumer<byte[], byte[]> consumer) {
            this.consumer = consumer;
        }

        void stop() {
            running = false;
            KafkaConsumer<byte[], byte[]> current = consumer;
            if (current != null) {
                current.wakeup();
            }
        }

        KafkaConsumer<byte[], byte[]> consumer() {
            return consumer;
        }

        boolean running() {
            return running;
        }
    }
}
