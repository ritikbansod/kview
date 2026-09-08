package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Schema registry endpoints: attach/detach per cluster, connectivity test,
 * schema browsing helpers and the decode endpoint used by the Data Explorer.
 */
@RestController
public class SchemaRegistryController {

    private final SchemaRegistryService registryService;

    public SchemaRegistryController(SchemaRegistryService registryService) {
        this.registryService = registryService;
    }

    public record DecodeRequest(@NotBlank String valueBase64, String topic, boolean key) { }

    // ---- attachment management ----

    @GetMapping("/api/clusters/{clusterId}/registry")
    public Map<String, Object> get(@PathVariable String clusterId) {
        SchemaRegistrySettings settings = registryService.settingsFor(clusterId)
                .orElseThrow(() -> new NoSuchElementException(
                        "No schema registry attached to cluster '" + clusterId + "'"));
        return Map.of("clusterId", clusterId, "registry", settings.masked(), "attached", true);
    }

    @PutMapping("/api/clusters/{clusterId}/registry")
    public Map<String, Object> attach(@PathVariable String clusterId,
                                      @Valid @RequestBody SchemaRegistrySettings settings) {
        registryService.save(clusterId, settings);
        SchemaRegistrySettings stored = registryService.settingsFor(clusterId)
                .orElseThrow(() -> new IllegalStateException("Attachment was not persisted"));
        return Map.of("clusterId", clusterId, "registry", stored.masked(), "status", "attached");
    }

    @DeleteMapping("/api/clusters/{clusterId}/registry")
    public Map<String, Object> detach(@PathVariable String clusterId) {
        boolean attached = registryService.settingsFor(clusterId).isPresent();
        registryService.delete(clusterId);
        return Map.of("clusterId", clusterId, "status", attached ? "detached" : "not-attached");
    }

    // ---- connectivity test (no cluster needed) ----

    @PostMapping("/api/registries/test")
    public Map<String, Object> test(@RequestBody SchemaRegistrySettings settings) {
        return registryService.test(settings);
    }

    // ---- schema browsing helpers ----

    @GetMapping("/api/clusters/{clusterId}/registry/subjects")
    public JsonNode subjects(@PathVariable String clusterId) {
        return registryService.subjects(clusterId);
    }

    @GetMapping("/api/clusters/{clusterId}/registry/subjects/{subject}/versions/{version}")
    public JsonNode schemaVersion(@PathVariable String clusterId, @PathVariable String subject,
                                  @PathVariable int version) {
        return registryService.schemaVersion(clusterId, subject, version);
    }

    // ---- decode ----

    @PostMapping("/api/clusters/{clusterId}/decode")
    public ResponseEntity<SchemaRegistryService.DecodedPayload> decode(
            @PathVariable String clusterId,
            @RequestParam(defaultValue = "false") boolean key,
            @RequestBody DecodeRequest request) {
        byte[] bytes;
        try {
            bytes = SchemaRegistryService.fromBase64(request.valueBase64());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("valueBase64 is not valid base64");
        }
        SchemaRegistryService.DecodedPayload payload =
                registryService.decode(clusterId, bytes, request.topic(), request.key());
        return ResponseEntity.status(HttpStatus.OK).body(payload);
    }

    // ---- encode (produce path) ----

    /** Encodes a JSON payload with the subject's schema into registry wire bytes. */
    @PostMapping("/api/clusters/{clusterId}/registry/subjects/{subject}/encode")
    public Map<String, Object> encode(@PathVariable String clusterId, @PathVariable String subject,
                                      @RequestParam(defaultValue = "false") boolean key,
                                      @Valid @RequestBody EncodeRequestHolder request) {
        if (request.payload() == null || request.payload().isNull()) {
            throw new IllegalArgumentException("payload is required");
        }
        SchemaRegistryService.EncodedPayload encoded = registryService.encode(clusterId, subject,
                request.version(), request.payload(), key, Boolean.TRUE.equals(request.dryRun()));
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("valueBase64", encoded.valueBase64());
        body.put("schemaId", encoded.schemaId());
        body.put("subject", encoded.subject());
        body.put("version", encoded.version());
        body.put("schemaType", encoded.schemaType());
        body.put("normalized", encoded.normalized());
        body.put("dryRun", Boolean.TRUE.equals(request.dryRun()));
        return body;
    }

    public record EncodeRequestHolder(JsonNode payload, Integer version, Boolean dryRun) { }

    /** Generates a sample JSON payload from the subject's schema. */
    @GetMapping("/api/clusters/{clusterId}/registry/subjects/{subject}/sample")
    public JsonNode sample(@PathVariable String clusterId, @PathVariable String subject,
                           @RequestParam(required = false) Integer version) {
        return registryService.sample(clusterId, subject, version);
    }
}
