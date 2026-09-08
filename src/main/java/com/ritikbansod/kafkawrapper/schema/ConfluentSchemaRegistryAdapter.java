package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

/**
 * Speaks the Confluent-compatible Schema Registry REST API — covers Confluent
 * Schema Registry / Cloud, Redpanda, Karapace, WarpStream and Apicurio's
 * Confluent-compat endpoints. Uses only the JDK HTTP client (no vendor dependency).
 */
@Component
public class ConfluentSchemaRegistryAdapter implements SchemaRegistryAdapter {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();

    @Override
    public String type() {
        return SchemaRegistrySettings.TYPE_CONFLUENT;
    }

    @Override
    public void test(SchemaRegistrySettings settings) {
        get(settings, "/subjects");
    }

    @Override
    public List<String> subjects(SchemaRegistrySettings settings) {
        try {
            JsonNode node = mapper.readTree(get(settings, "/subjects"));
            return mapper.convertValue(node, mapper.getTypeFactory().constructCollectionType(List.class, String.class));
        } catch (Exception e) {
            throw new IllegalStateException("Could not list registry subjects: " + rootMessage(e), e);
        }
    }

    @Override
    public JsonNode versions(SchemaRegistrySettings settings, String subject) {
        return getJson(settings, "/subjects/" + encode(subject) + "/versions");
    }

    @Override
    public JsonNode schemaVersion(SchemaRegistrySettings settings, String subject, int version) {
        return getJson(settings, "/subjects/" + encode(subject) + "/versions/" + version);
    }

    @Override
    public JsonNode byId(SchemaRegistrySettings settings, int id) {
        return getJson(settings, "/schemas/ids/" + id);
    }

    /** Raw authenticated GET against any registry path (encode/compat plumbing). */
    public JsonNode rawGet(SchemaRegistrySettings settings, String path) {
        return getJson(settings, path);
    }

    private JsonNode getJson(SchemaRegistrySettings settings, String path) {
        try {
            return mapper.readTree(get(settings, path));
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Schema registry call failed: " + rootMessage(e), e);
        }
    }

    private String get(SchemaRegistrySettings settings, String path) {
        String url = settings.url().replaceAll("/+$", "") + path;
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .GET();
        if (SchemaRegistrySettings.AUTH_BASIC.equals(settings.authTypeOrDefault())) {
            String credentials = Base64.getEncoder().encodeToString(
                    ((settings.username() == null ? "" : settings.username()) + ":"
                            + (settings.password() == null ? "" : settings.password()))
                            .getBytes(StandardCharsets.UTF_8));
            builder.header("Authorization", "Basic " + credentials);
        }
        try {
            HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("Schema registry returned HTTP " + response.statusCode()
                        + " for " + path + ": " + response.body().substring(0, Math.min(response.body().length(), 200)));
            }
            return response.body();
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Schema registry unreachable at " + url + ": " + rootMessage(e), e);
        }
    }

    private static String encode(String subject) {
        return java.net.URLEncoder.encode(subject, StandardCharsets.UTF_8);
    }

    private static String rootMessage(Throwable e) {
        while (e.getCause() != null) e = e.getCause();
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }
}
