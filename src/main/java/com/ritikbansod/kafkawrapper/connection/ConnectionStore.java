package com.ritikbansod.kafkawrapper.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Persists connection profiles as a single JSON file under the data directory.
 * Intentionally file-based (no DB dependency) so Kview stays a single
 * self-contained deployment artifact. Writes are atomic (temp file + move).
 */
@Component
public class ConnectionStore {

    private static final Logger log = LoggerFactory.getLogger(ConnectionStore.class);

    private final Path file;
    private final ObjectMapper mapper;
    private final List<ConnectionProfile> profiles = new CopyOnWriteArrayList<>();

    public ConnectionStore(@Value("${kview.data-dir:./data}") String dataDir,
                           ObjectMapper mapper) {
        this.file = Path.of(dataDir, "connections.json");
        this.mapper = mapper;
        load();
    }

    public List<ConnectionProfile> all() {
        return List.copyOf(profiles);
    }

    public Optional<ConnectionProfile> find(String id) {
        return profiles.stream().filter(p -> p.id().equals(id)).findFirst();
    }

    public synchronized ConnectionProfile save(ConnectionProfile profile) {
        ConnectionProfile toStore = profile.id() == null || profile.id().isBlank()
                ? profile.withId("c-" + UUID.randomUUID().toString().substring(0, 8))
                : profile;
        profiles.removeIf(p -> p.id().equals(toStore.id()));
        profiles.add(toStore);
        persist();
        return toStore;
    }

    public synchronized boolean delete(String id) {
        boolean removed = profiles.removeIf(p -> p.id().equals(id));
        if (removed) {
            persist();
        }
        return removed;
    }

    private void load() {
        if (!Files.exists(file)) {
            return;
        }
        try {
            ConnectionProfile[] parsed = mapper.readValue(file.toFile(), ConnectionProfile[].class);
            profiles.clear();
            profiles.addAll(List.of(parsed));
            log.info("Loaded {} connection profile(s) from {}", profiles.size(), file);
        } catch (IOException e) {
            Path broken = file.resolveSibling("connections.json.broken-" + System.currentTimeMillis());
            log.error("Connections file {} is unreadable ({}). Moving it aside to {} and starting empty.",
                    file, e.getMessage(), broken);
            try {
                Files.move(file, broken, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException moveError) {
                log.error("Could not move broken connections file aside", moveError);
            }
            profiles.clear();
        }
    }

    private void persist() {
        try {
            Files.createDirectories(file.getParent());
            Path temp = file.resolveSibling(file.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), profiles);
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            log.error("Failed to persist connections to {}: {}", file, e.getMessage());
        }
    }
}
