package com.ritikbansod.kafkawrapper.replay;

import com.ritikbansod.kafkawrapper.browse.MessageBrowserService;
import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Bulk replay (DLQ triage): select messages from a source topic with the
 * browser's read-only scan, then copy them — keys, headers and original
 * timestamps preserved — to another topic on the same or a different connected
 * cluster. Bounded per call (max 1000, oldest first); a dry run only selects.
 * Mutations through this path are admin-only, audited and refused in
 * read-only mode by the standard safety chain.
 */
@Service
public class ReplayService {

    private static final int MAX_PER_CALL = 1000;

    private final MessageBrowserService browser;
    private final KafkaClusterManager manager;

    public ReplayService(MessageBrowserService browser, KafkaClusterManager manager) {
        this.browser = browser;
        this.manager = manager;
    }

    public record ReplayRequest(String targetClusterId, String targetTopic, boolean copyHeaders,
                                boolean dryRun, Integer limit, String keyContains, String valueContains) { }

    public record ReplayResult(String sourceTopic, String targetClusterId, String targetTopic,
                               boolean dryRun, int copied, int selected, boolean reachedEnd,
                               List<String> errors) { }

    public ReplayResult replay(String clusterId, String topic, ReplayRequest request) {
        String targetTopic = request.targetTopic() == null || request.targetTopic().isBlank()
                ? topic : request.targetTopic();
        String targetClusterId = request.targetClusterId() == null || request.targetClusterId().isBlank()
                ? clusterId : request.targetClusterId();
        int limit = request.limit() == null ? 500 : Math.min(Math.max(request.limit(), 1), MAX_PER_CALL);

        ClusterHandle source = manager.get(clusterId);
        var selection = browser.browse(source, topic, new MessageBrowserService.BrowseRequest(
                null, "earliest", null, null, limit, 60_000L, request.keyContains(), request.valueContains()));

        List<String> errors = new ArrayList<>();
        int copied = 0;
        if (!request.dryRun()) {
            ClusterHandle target = manager.get(targetClusterId);
            for (MessageBrowserService.BrowserMessage message : selection.messages()) {
                try {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(targetTopic, null, message.timestamp(),
                            message.keyBase64() == null ? null : Base64.getDecoder().decode(message.keyBase64()),
                            message.valueBase64() == null ? null : Base64.getDecoder().decode(message.valueBase64()));
                    if (request.copyHeaders()) {
                        message.headers().forEach((key, value) ->
                                record.headers().add(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8)));
                    }
                    target.template().send(record).get(15, TimeUnit.SECONDS);
                    copied++;
                } catch (Exception e) {
                    errors.add(message.topic() + " p" + message.partition() + "@" + message.offset()
                            + ": " + e.getMessage());
                    break; // fail fast — the caller can re-run with an offset/filter to continue
                }
            }
        }
        return new ReplayResult(topic, targetClusterId, targetTopic, request.dryRun(),
                copied, selection.messages().size(), selection.reachedEnd(), errors);
    }
}
