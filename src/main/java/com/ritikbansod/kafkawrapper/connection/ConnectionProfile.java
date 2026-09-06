package com.ritikbansod.kafkawrapper.connection;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * A named Kafka cluster connection. One wrapper instance can talk to any number
 * of clusters; connections are managed at runtime and persisted server-side.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConnectionProfile(
        String id,
        String name,
        List<String> bootstrapServers,
        SecuritySettings security) {

    public ConnectionProfile {
        if (bootstrapServers == null) {
            bootstrapServers = List.of();
        } else {
            bootstrapServers = List.copyOf(bootstrapServers);
        }
    }

    public String displayName() {
        return name == null || name.isBlank() ? id : name;
    }

    public ConnectionProfile withId(String newId) {
        return new ConnectionProfile(newId, name, bootstrapServers, security);
    }
}
