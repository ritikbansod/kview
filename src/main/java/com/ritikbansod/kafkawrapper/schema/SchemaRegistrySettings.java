package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Connection settings for an attached schema registry (phase 1: Confluent-compatible).
 * Stored inside a {@link com.ritikbansod.kafkawrapper.connection.ConnectionProfile};
 * secrets are masked in API responses with the same policy as broker connections.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SchemaRegistrySettings(
        String type,                     // CONFLUENT (only type in phase 1)
        String url,
        String authType,                 // NONE | BASIC
        String username,
        String password) {

    public static final String TYPE_CONFLUENT = "CONFLUENT";
    public static final String AUTH_NONE = "NONE";
    public static final String AUTH_BASIC = "BASIC";

    public static SchemaRegistrySettings confluent(String url) {
        return new SchemaRegistrySettings(TYPE_CONFLUENT, url, AUTH_NONE, null, null);
    }

    public boolean enabled() {
        return type != null && !type.isBlank() && url != null && !url.isBlank();
    }

    public String authTypeOrDefault() {
        return authType == null || authType.isBlank() ? AUTH_NONE : authType;
    }

    /** Masked copy for API responses; masked markers mean "keep stored value" on edit. */
    public SchemaRegistrySettings masked() {
        return new SchemaRegistrySettings(type, url, authType,
                username,
                password == null || password.isBlank() ? null : "••••••");
    }
}
