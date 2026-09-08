package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Persists per-cluster schema-registry attachments to {@code registries.json}
 * in the data directory (works for the built-in default cluster and for
 * stored connection profiles alike).
 */
@Component
public class RegistrySettingsStore {

    private static final Logger log = LoggerFactory.getLogger(RegistrySettingsStore.class);

    private final Path file;
    private final ObjectMapper mapper;
    private final Map<String, SchemaRegistrySettings> byCluster = new LinkedHashMap<>();

    public RegistrySettingsStore(@Value("${kview.data-dir:./data}") String dataDir,
                                 ObjectMapper mapper) {
        this.file = Path.of(dataDir, "registries.json");
        this.mapper = mapper;
        load();
    }

    public Optional<SchemaRegistrySettings> find(String clusterId) {
        SchemaRegistrySettings settings = byCluster.get(clusterId);
        return settings != null && settings.enabled() ? Optional.of(settings) : Optional.empty();
    }

    public synchronized void put(String clusterId, SchemaRegistrySettings settings) {
        byCluster.put(clusterId, settings);
        persist();
    }

    public synchronized boolean delete(String clusterId) {
        boolean removed = byCluster.remove(clusterId) != null;
        if (removed) persist();
        return removed;
    }

    /** Keeps the stored password when an incoming masked marker arrives. */
    public synchronized SchemaRegistrySettings mergeSecrets(String clusterId, SchemaRegistrySettings incoming) {
        if (incoming == null) return null;
        SchemaRegistrySettings stored = byCluster.get(clusterId);
        if (stored == null) return incoming;
        boolean masked = "••••••".equals(incoming.password());
        return new SchemaRegistrySettings(incoming.type(), incoming.url(), incoming.authType(),
                incoming.username(), masked ? stored.password() : incoming.password());
    }

    private void load() {
        if (!Files.exists(file)) return;
        try {
            Map<String, SchemaRegistrySettings> parsed = mapper.readValue(file.toFile(),
                    mapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, SchemaRegistrySettings.class));
            byCluster.putAll(parsed);
            log.info("Loaded {} schema registry attachment(s) from {}", byCluster.size(), file);
        } catch (IOException e) {
            log.error("Registry settings file {} is unreadable — starting empty", file, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(file.getParent());
            Path temp = file.resolveSibling(file.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), byCluster);
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            log.error("Failed to persist registry settings to {}: {}", file, e.getMessage());
        }
    }
}
