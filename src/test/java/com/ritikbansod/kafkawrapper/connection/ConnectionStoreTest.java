package com.ritikbansod.kafkawrapper.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionStoreTest {

    @TempDir
    Path tempDir;

    private ConnectionProfile profile(String id, String name) {
        return new ConnectionProfile(id, name, List.of("broker-1:9092", "broker-2:9092"),
                SecuritySettings.plaintext());
    }

    @Test
    void saveFindAndDeleteRoundtrip() {
        ConnectionStore store = new ConnectionStore(tempDir.toString(), new ObjectMapper());

        ConnectionProfile saved = store.save(profile(null, "dev"));   // id assigned
        store.save(profile("fixed", "prod"));

        assertThat(store.all()).hasSize(2);
        assertThat(saved.id()).isNotBlank();
        assertThat(store.find(saved.id())).isPresent();
        assertThat(store.find("fixed")).map(ConnectionProfile::displayName).contains("prod");

        assertThat(store.delete("fixed")).isTrue();
        assertThat(store.find("fixed")).isEmpty();
        assertThat(store.delete("fixed")).isFalse();
    }

    @Test
    void profilesSurviveAReopen() throws Exception {
        ConnectionStore first = new ConnectionStore(tempDir.toString(), new ObjectMapper());
        first.save(profile("persisted", "staging"));

        assertThat(tempDir.resolve("connections.json")).exists();

        ConnectionStore second = new ConnectionStore(tempDir.toString(), new ObjectMapper());
        assertThat(second.find("persisted")).isPresent();
        assertThat(second.find("persisted").orElseThrow().bootstrapServers())
                .containsExactly("broker-1:9092", "broker-2:9092");
    }

    @Test
    void corruptFileStartsEmptyInsteadOfCrashing() throws Exception {
        Files.createDirectories(tempDir);
        Files.writeString(tempDir.resolve("connections.json"), "{ this is not json");

        ConnectionStore store = new ConnectionStore(tempDir.toString(), new ObjectMapper());
        assertThat(store.all()).isEmpty();
        // and saving still works afterwards
        assertThat(store.save(profile("after-crash", "qa")).id()).isEqualTo("after-crash");
    }
}
