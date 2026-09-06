package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schema-registry orchestration: settings lookup per cluster, schema caching,
 * subject resolution and the decode pipeline (bytes → JSON) for the Confluent
 * wire format. Unknown formats are reported, never guessed.
 */
@Service
public class SchemaRegistryService {

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryService.class);

    public record DecodedPayload(String wireFormat, Long schemaId, String schemaType,
                                 String subject, Integer version,
                                 JsonNode decoded, String note, String error) {

        public static DecodedPayload unknown(String note) {
            return new DecodedPayload("UNKNOWN", null, null, null, null, null, note, null);
        }

        public static DecodedPayload failure(String note, String error) {
            return new DecodedPayload("UNKNOWN", null, null, null, null, null, note, error);
        }
    }

    private record CacheEntry(Object value, long expiresAtMs) { }

    private static final long SCHEMA_TTL_MS = 300_000;     // 5 min for schemas by id
    private static final long SUBJECTS_TTL_MS = 60_000;    // 1 min for subject lists
    private static final long AVRO_TTL_MS = 300_000;       // parsed Avro schemas

    private final List<SchemaRegistryAdapter> adapters;
    private final RegistrySettingsStore settingsStore;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public SchemaRegistryService(List<SchemaRegistryAdapter> adapters, RegistrySettingsStore settingsStore) {
        this.adapters = adapters;
        this.settingsStore = settingsStore;
    }

    public Optional<SchemaRegistrySettings> settingsFor(String clusterId) {
        return settingsStore.find(clusterId);
    }

    public void save(String clusterId, SchemaRegistrySettings settings) {
        settingsStore.put(clusterId, settingsStore.mergeSecrets(clusterId, settings));
    }

    public void delete(String clusterId) {
        settingsStore.delete(clusterId);
    }

    private SchemaRegistryAdapter adapterFor(SchemaRegistrySettings settings) {
        return adapters.stream()
                .filter(a -> a.type().equalsIgnoreCase(settings.type()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No schema registry adapter for type '" + settings.type() + "'"));
    }

    /** Connectivity probe used by the connections UI. */
    public Map<String, Object> test(SchemaRegistrySettings settings) {
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        try {
            SchemaRegistryAdapter adapter = adapterFor(settings);
            adapter.test(settings);
            List<String> subjects = adapter.subjects(settings);
            result.put("ok", true);
            result.put("type", settings.type());
            result.put("subjectCount", subjects.size());
        } catch (Exception e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    // ---------- decode pipeline ----------

    /**
     * Decodes a raw payload: sniffs the wire format, resolves the schema (cached),
     * and renders readable JSON. Returns a {@link DecodedPayload} — never throws.
     */
    public DecodedPayload decode(String clusterId, byte[] payload, String topic, boolean isKey) {
        Optional<SchemaRegistrySettings> settings = settingsFor(clusterId);
        if (settings.isEmpty()) {
            return DecodedPayload.unknown("No schema registry attached to this cluster");
        }
        var header = WireFormatSniffer.sniff(payload);
        if (header.isEmpty()) {
            return DecodedPayload.unknown("Payload has no recognized wire-format header");
        }
        if (header.get().format() != WireFormatSniffer.Format.CONFLUENT) {
            return DecodedPayload.unknown("Wire format not supported yet: " + header.get().format());
        }
        int schemaId = (int) header.get().schemaId();

        try {
            JsonNode schemaEnvelope = cached("schema-id:" + settings.get().url() + ":" + schemaId,
                    SCHEMA_TTL_MS, () -> adapterFor(settings.get()).byId(settings.get(), schemaId));
            String schemaType = schemaEnvelope.path("schemaType").asText("AVRO");
            String schemaJson = schemaEnvelope.path("schema").asText();
            String subject = resolveSubject(settings.get(), topic, isKey, schemaId);

            ObjectNode result = JsonNodeFactory.instance.objectNode();
            result.put("wireFormat", "CONFLUENT");
            result.put("schemaId", schemaId);
            result.put("schemaType", schemaType);
            if (subject != null) result.put("subject", subject);

            switch (schemaType.toUpperCase()) {
                case "AVRO" -> {
                    JsonNode decoded = avroToJson(payload, schemaJson);
                    result.set("decoded", decoded);
                }
                case "JSON" -> {
                    // JSON-Schema payloads are JSON text after the header
                    String text = new String(payload, 5, payload.length - 5, StandardCharsets.UTF_8);
                    result.set("decoded", AvroJsonConverter.fromJson(text));
                }
                default -> {
                    result.put("note", "Decoding for '" + schemaType + "' arrives in phase 2 — showing raw payload");
                    result.set("decoded", AvroJsonConverter.fromJson(text(payload)));
                }
            }
            return new DecodedPayload("CONFLUENT", (long) schemaId, schemaType, subject, null,
                    result.get("decoded"), null, null);
        } catch (Exception e) {
            log.warn("Decode failed for topic {}: {}", topic, e.getMessage(), e);
            return DecodedPayload.failure("Decode failed",
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private String text(byte[] payload) {
        return new String(payload, 5, Math.max(0, payload.length - 5), StandardCharsets.UTF_8);
    }

    /** Resolves the subject via the topic naming strategy; falls back to scanning subjects for the schema id. */
    private String resolveSubject(SchemaRegistrySettings settings, String topic, boolean isKey, int schemaId) {
        String suffix = isKey ? "-key" : "-value";
        try {
            List<String> subjects = cached("subjects:" + settings.url(), SUBJECTS_TTL_MS,
                    () -> adapterFor(settings).subjects(settings));
            if (topic != null) {
                String direct = topic + suffix;
                if (subjects.contains(direct)) return direct;
            }
            return null; // strategy other than TopicName — subject shown as unknown in phase 1
        } catch (Exception e) {
            return null;
        }
    }

    private JsonNode avroToJson(byte[] payload, String schemaJson) throws Exception {
        Schema schema = cached("avro:" + schemaJson.hashCode(), AVRO_TTL_MS, () -> {
            try {
                return new Schema.Parser().parse(schemaJson);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Could not parse Avro schema: " + e.getMessage(), e);
            }
        });
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord record = reader.read(null,
                DecoderFactory.get().binaryDecoder(payload, 5, payload.length - 5, null));
        return AvroJsonConverter.toJson(record, schema);
    }

    // ---------- tiny TTL cache ----------

    @SuppressWarnings("unchecked")
    private <T> T cached(String key, long ttlMs, java.util.function.Supplier<T> loader) {
        CacheEntry entry = cache.get(key);
        if (entry != null && entry.expiresAtMs() > System.currentTimeMillis()) {
            return (T) entry.value();
        }
        synchronized (this) {
            entry = cache.get(key);
            if (entry != null && entry.expiresAtMs() > System.currentTimeMillis()) {
                return (T) entry.value();
            }
            T value = loader.get();
            cache.put(key, new CacheEntry(value, System.currentTimeMillis() + ttlMs));
            return value;
        }
    }

    /** Content of a schema version as raw JSON text (for the upcoming Schemas page). */
    public JsonNode schemaVersion(String clusterId, String subject, int version) {
        SchemaRegistrySettings settings = settingsFor(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("No schema registry attached to cluster '" + clusterId + "'"));
        return adapterFor(settings).schemaVersion(settings, subject, version);
    }

    public JsonNode subjects(String clusterId) {
        SchemaRegistrySettings settings = settingsFor(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("No schema registry attached to cluster '" + clusterId + "'"));
        return mapper.valueToTree(adapterFor(settings).subjects(settings));
    }

    /** Base64 helper reused by controllers. */
    public static byte[] fromBase64(String base64) {
        return Base64.getDecoder().decode(base64);
    }
}
