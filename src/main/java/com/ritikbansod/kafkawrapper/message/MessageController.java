package com.ritikbansod.kafkawrapper.message;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
public class MessageController {

    private final ProducerService producerService;
    private final KafkaClusterManager manager;

    public MessageController(ProducerService producerService, KafkaClusterManager manager) {
        this.producerService = producerService;
        this.manager = manager;
    }

    public record ProduceMessageRequest(
            @NotBlank String topic,
            @Size(max = 256) String key,
            String payload,
            String payloadBase64,
            Integer partition,
            Long timestamp,
            Map<String, String> headers) {
    }

    public record ProduceBody(
            @Size(max = 256) String key,
            String payload,
            String payloadBase64,
            Integer partition,
            Long timestamp,
            Map<String, String> headers) {
    }

    // ---- cluster-scoped API ----

    /** Produce directly into a named topic (topic comes from the path). */
    @PostMapping("/api/clusters/{clusterId}/topics/{topic}/messages")
    public ResponseEntity<Map<String, Object>> produceToTopic(
            @PathVariable String clusterId, @PathVariable String topic,
            @Valid @RequestBody ProduceBody body)
            throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> result = producerService.produce(manager.get(clusterId),
                topic, body.key(), body.payload(), body.payloadBase64(), body.partition(), body.timestamp(), body.headers());
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }

    /** Produce with the topic given in the body. */
    @PostMapping("/api/clusters/{clusterId}/messages")
    public ResponseEntity<Map<String, Object>> produce(@PathVariable String clusterId,
                                                       @Valid @RequestBody ProduceMessageRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> result = producerService.produce(manager.get(clusterId),
                request.topic(), request.key(), request.payload(), request.payloadBase64(), request.partition(),
                request.timestamp(), request.headers());
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }

    // ---- legacy API (backward compatible, targets the default cluster) ----

    @PostMapping("/api/messages")
    public ResponseEntity<Map<String, Object>> produceLegacy(@Valid @RequestBody ProduceMessageRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        return produce(KafkaClusterManager.DEFAULT_CLUSTER_ID, request);
    }
}
