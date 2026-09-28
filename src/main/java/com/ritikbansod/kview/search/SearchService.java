package com.ritikbansod.kview.search;

import com.ritikbansod.kview.browse.MessageBrowserService;
import com.ritikbansod.kview.connection.KafkaClusterManager;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Whole-topic background search: assigns every partition (or one), seeks to the
 * optional from-timestamp, and scans forward up to a snapshot end offset,
 * collecting matches — read-only like the browser (own throwaway group, never
 * commits). Bounded: results capped, scanned messages capped, a small pool of
 * concurrent jobs, jobs expire from memory.
 */
@Service
public class SearchService {

    private static final Logger log = LoggerFactory.getLogger(SearchService.class);
    private static final int MAX_CONCURRENT_JOBS = 3;
    private static final int MAX_RESULTS = 1000;
    private static final long MAX_SCANNED = 2_000_000;
    private static final long JOB_TTL_MS = 30 * 60_000L;

    private final KafkaClusterManager manager;
    private final MessageBrowserService browser;
    private final ExecutorService pool = Executors.newFixedThreadPool(2, r -> {
        Thread thread = new Thread(r, "kview-search");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<String, SearchJob> jobs = new ConcurrentHashMap<>();

    public SearchService(KafkaClusterManager manager, MessageBrowserService browser) {
        this.manager = manager;
        this.browser = browser;
    }

    public record SearchRequest(Long fromTs, Long toTs, String keyContains, String valueContains,
                                String valueRegex, Integer partition, Integer limit) { }

    public record SearchStatus(String searchId, String topic, String state, String error,
                               int partitionsTotal, int partitionsDone, long scanned, long matched,
                               boolean limitReached, long startedAt, long finishedAt,
                               List<MessageBrowserService.BrowserMessage> results) { }

    /** Starts a search and returns its id. */
    public synchronized String start(String clusterId, String topic, SearchRequest request) {
        long running = jobs.values().stream().filter(j -> "RUNNING".equals(j.state)).count();
        if (running >= MAX_CONCURRENT_JOBS) {
            throw new IllegalArgumentException(
                    "Too many concurrent searches (" + MAX_CONCURRENT_JOBS + ") — wait or cancel one first");
        }
        if (request.valueRegex() != null && !request.valueRegex().isBlank()) {
            Pattern.compile(request.valueRegex()); // invalid regex fails fast with a 400
        }
        String id = UUID.randomUUID().toString().substring(0, 8);
        SearchJob job = new SearchJob(id, clusterId, topic, request);
        job.limit = request.limit() == null ? 100 : Math.min(Math.max(request.limit(), 1), MAX_RESULTS);
        jobs.put(id, job);
        job.future = pool.submit(() -> run(job));
        log.info("Search {} started on cluster '{}' topic '{}'", id, clusterId, topic);
        return id;
    }

    public SearchStatus status(String clusterId, String searchId) {
        SearchJob job = job(clusterId, searchId);
        List<MessageBrowserService.BrowserMessage> snapshot;
        synchronized (job.results) {
            snapshot = List.copyOf(job.results);
        }
        return new SearchStatus(job.id, job.topic, job.state, job.error,
                job.partitionsTotal.get(), job.partitionsDone.get(),
                job.scanned.get(), job.matched.get(), job.matched.get() >= job.limit,
                job.startedAt, job.finishedAt, snapshot);
    }

    public void cancel(String clusterId, String searchId) {
        SearchJob job = job(clusterId, searchId);
        job.cancelled.set(true);
        KafkaConsumer<?, ?> consumer = job.consumer;
        if (consumer != null) consumer.wakeup();
    }

    /** Drops finished jobs from memory after their TTL. */
    @Scheduled(fixedDelay = 60_000)
    void cleanup() {
        long now = System.currentTimeMillis();
        jobs.entrySet().removeIf(e -> !"RUNNING".equals(e.getValue().state)
                && now - e.getValue().startedAt > JOB_TTL_MS);
    }

    private SearchJob job(String clusterId, String searchId) {
        SearchJob job = jobs.get(searchId);
        if (job == null || !job.clusterId.equals(clusterId)) {
            throw new NoSuchElementException("Unknown search id '" + searchId + "'");
        }
        return job;
    }

    private void run(SearchJob job) {
        SearchRequest request = job.request;
        String clusterId = job.clusterId;
        int limit = job.limit;
        Pattern regex = request.valueRegex() == null || request.valueRegex().isBlank()
                ? null : Pattern.compile(request.valueRegex());
        String keyContains = lower(request.keyContains());
        String valueContains = lower(request.valueContains());

        Map<String, Object> props;
        try {
            props = new HashMap<>(manager.get(clusterId).consumerProperties());
        } catch (Exception e) {
            job.finish("FAILED", e.getMessage());
            return;
        }
        // same read-only contract as the browser: throwaway group, never commits
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kview-search-" + job.id);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            job.consumer = consumer;
            List<TopicPartition> partitions = partitions(consumer, job.topic, request.partition());
            if (partitions.isEmpty()) {
                throw new org.apache.kafka.common.errors.UnknownTopicOrPartitionException(job.topic);
            }
            job.partitionsTotal.set(partitions.size());

            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(partitions);
            consumer.assign(partitions);
            for (TopicPartition tp : partitions) {
                if (request.fromTs() != null) {
                    OffsetAndTimestamp at = consumer.offsetsForTimes(
                            Map.of(tp, request.fromTs())).get(tp);
                    consumer.seek(tp, at == null ? ends.get(tp) : Math.max(at.offset(), beginnings.getOrDefault(tp, 0L)));
                } else {
                    consumer.seek(tp, beginnings.getOrDefault(tp, 0L));
                }
            }

            while (!job.cancelled.get()
                    && job.scanned.get() < MAX_SCANNED
                    && job.matched.get() < limit
                    && job.partitionsDone.get() < partitions.size()) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(250))) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (job.donePartitions.containsKey(tp)) continue;
                    if (request.toTs() != null && record.timestamp() > request.toTs()) {
                        markDone(job, tp);
                        continue;
                    }
                    job.scanned.incrementAndGet();
                    if (matches(record, keyContains, valueContains, regex)) {
                        synchronized (job.results) {
                            job.results.add(browser.toView(clusterId, record));
                        }
                        job.matched.incrementAndGet();
                        if (job.matched.get() >= limit) break;
                    }
                }
                // a partition is done when the scan reached its snapshot end offset
                for (TopicPartition tp : partitions) {
                    if (!job.donePartitions.containsKey(tp)
                            && consumer.position(tp) >= ends.getOrDefault(tp, 0L)) {
                        markDone(job, tp);
                    }
                }
            }
            job.finish(job.cancelled.get() ? "CANCELLED" : "DONE", null);
        } catch (WakeupException e) {
            job.finish("CANCELLED", null);
        } catch (Exception e) {
            log.warn("Search {} failed: {}", job.id, e.getMessage());
            job.finish("FAILED", e.getMessage());
        }
    }

    private void markDone(SearchJob job, TopicPartition tp) {
        if (job.donePartitions.putIfAbsent(tp, true) == null) {
            job.partitionsDone.incrementAndGet();
        }
    }

    private boolean matches(ConsumerRecord<byte[], byte[]> record, String keyContains, String valueContains,
                            Pattern regex) {
        String key = MessageBrowserService.asText(record.key());
        String value = MessageBrowserService.asText(record.value());
        if (keyContains != null && (key == null || !key.toLowerCase().contains(keyContains))) return false;
        if (valueContains != null && (value == null || !value.toLowerCase().contains(valueContains))) return false;
        return regex == null || (value != null && regex.matcher(value).find());
    }

    private List<TopicPartition> partitions(KafkaConsumer<byte[], byte[]> consumer, String topic, Integer only) {
        List<TopicPartition> result = new ArrayList<>();
        var infos = consumer.partitionsFor(topic, Duration.ofSeconds(10));
        if (infos != null) {
            infos.forEach(info -> {
                if (only == null || only == info.partition()) {
                    result.add(new TopicPartition(topic, info.partition()));
                }
            });
        }
        return result;
    }

    private static String lower(String s) {
        return s == null || s.isBlank() ? null : s.toLowerCase();
    }

    @PreDestroy
    void shutdown() {
        pool.shutdownNow();
    }

    private static final class SearchJob {
        final String id;
        final String clusterId;
        final String topic;
        final SearchRequest request;
        int limit = 100;
        final long startedAt = System.currentTimeMillis();
        final AtomicLong scanned = new AtomicLong();
        final AtomicLong matched = new AtomicLong();
        final AtomicInteger partitionsTotal = new AtomicInteger();
        final AtomicInteger partitionsDone = new AtomicInteger();
        final AtomicBoolean cancelled = new AtomicBoolean();
        final Map<TopicPartition, Boolean> donePartitions = new ConcurrentHashMap<>();
        final List<MessageBrowserService.BrowserMessage> results = Collections.synchronizedList(new ArrayList<>());
        volatile String state = "RUNNING";
        volatile String error;
        volatile long finishedAt;
        volatile KafkaConsumer<byte[], byte[]> consumer;
        volatile Future<?> future;

        SearchJob(String id, String clusterId, String topic, SearchRequest request) {
            this.id = id;
            this.clusterId = clusterId;
            this.topic = topic;
            this.request = request;
        }

        void finish(String state, String error) {
            if ("RUNNING".equals(this.state)) {
                this.state = state;
                this.error = error;
                this.finishedAt = System.currentTimeMillis();
            }
        }
    }
}
