package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * SPI for schema registries. Phase 1 ships the Confluent-compatible adapter
 * (Confluent SR/Cloud, Redpanda, Karapace, Apicurio compat mode, WarpStream);
 * later phases add Apicurio-native, AWS Glue and Azure adapters.
 */
public interface SchemaRegistryAdapter {

    /** Registry type this adapter handles (matches SchemaRegistrySettings.type). */
    String type();

    /** Connectivity + auth probe. Throws on failure. */
    void test(SchemaRegistrySettings settings);

    /** All subject names visible to the credentials. */
    List<String> subjects(SchemaRegistrySettings settings);

    /** Version list for a subject: [{subject, version, schemaType?, id?}]. */
    JsonNode versions(SchemaRegistrySettings settings, String subject);

    /** One version: {subject, version, schemaType, schema, references?}. */
    JsonNode schemaVersion(SchemaRegistrySettings settings, String subject, int version);

    /** Raw schema content by numeric wire ID: {schemaType, schema}. */
    JsonNode byId(SchemaRegistrySettings settings, int id);
}
