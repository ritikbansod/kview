package com.ritikbansod.kview.schema;

import com.google.protobuf.DynamicMessage;
import com.sun.net.httpserver.HttpServer;
import com.ritikbansod.kview.browse.MessageBrowserService;
import com.ritikbansod.kview.connection.ClusterHandle;
import com.ritikbansod.kview.connection.KafkaClusterManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 2b acceptance: a Confluent-compatible registry (mocked over HTTP)
 * serves a PROTOBUF schema; hand-built Confluent wire bytes decode to proto3
 * JSON, and JSON payloads encode back into registry wire bytes — index prefix
 * included. The wire fixture uses the Confluent shape:
 * 0x00 + schema id + message-index prefix (1-based varints, 0x00-terminated)
 * + protobuf message bytes.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = ProtobufSchemaIntegrationTest.TOPIC)
class ProtobufSchemaIntegrationTest {

    static final String TOPIC = "pb-orders";
    static final String SUBJECT = "pb-orders-value";
    static final int SCHEMA_ID = 7;

    static final String PROTO = """
            syntax = "proto3";
            message PbOrder {
              string order_id = 1;
              double amount = 2;
              bool paid = 3;
            }
            """;

    private static HttpServer fakeRegistry;
    private static String fakeRegistryUrl;

    @Autowired
    SchemaRegistryService registryService;

    @Autowired
    KafkaClusterManager clusterManager;

    @DynamicPropertySource
    static void kafkaBootstrap(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
        registry.add("kview.data-dir", () -> "target/protobuf-test-data");
    }

    @BeforeEach
    void startFakeRegistryAndAttach() throws Exception {
        fakeRegistry = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeRegistry.createContext("/schemas/ids/" + SCHEMA_ID, exchange -> {
            byte[] body = protoEnvelope().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        fakeRegistry.createContext("/subjects/" + SUBJECT + "/versions/latest", exchange -> {
            byte[] body = protoEnvelope().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        fakeRegistry.createContext("/subjects", exchange -> {
            byte[] body = ("[\"" + SUBJECT + "\"]").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        fakeRegistry.start();
        fakeRegistryUrl = "http://127.0.0.1:" + fakeRegistry.getAddress().getPort();

        registryService.save(KafkaClusterManager.DEFAULT_CLUSTER_ID,
                SchemaRegistrySettings.confluent(fakeRegistryUrl));
    }

    @AfterEach
    void detachAndStop() {
        registryService.delete(KafkaClusterManager.DEFAULT_CLUSTER_ID);
        if (fakeRegistry != null) fakeRegistry.stop(0);
    }

    private static String protoEnvelope() {
        return "{\"schemaType\":\"PROTOBUF\",\"id\":" + SCHEMA_ID
                + ",\"schema\":" + com.fasterxml.jackson.databind.node.TextNode.valueOf(PROTO).toString() + "}";
    }

    private byte[] header(int schemaId) {
        return new byte[]{0x00,
                (byte) ((schemaId >>> 24) & 0xFF), (byte) ((schemaId >>> 16) & 0xFF),
                (byte) ((schemaId >>> 8) & 0xFF), (byte) (schemaId & 0xFF)};
    }

    /** Builds the protobuf message bytes through the same codec the service uses. */
    private byte[] protobufMessage(String orderId, double amount, boolean paid) throws Exception {
        var compiled = ProtobufCodec.compile(PROTO);
        var descriptor = compiled.schema().getMessageDescriptor("PbOrder");
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("order_id"), orderId)
                .setField(descriptor.findFieldByName("amount"), amount)
                .setField(descriptor.findFieldByName("paid"), paid)
                .build()
                .toByteArray();
    }

    @Test
    void confluentWireBytesDecodeToProto3Json() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(header(SCHEMA_ID));
        out.write(new byte[]{0x01, 0x00}); // message-index prefix: top-level message 0
        out.write(protobufMessage("ORD-42", 91.5, true));

        var decoded = registryService.decode(KafkaClusterManager.DEFAULT_CLUSTER_ID,
                out.toByteArray(), TOPIC, false);

        assertThat(decoded.wireFormat()).isEqualTo("CONFLUENT");
        assertThat(decoded.error()).isNull();
        assertThat(decoded.decoded().get("order_id").asText()).isEqualTo("ORD-42");
        assertThat(decoded.decoded().get("amount").asDouble()).isEqualTo(91.5);
        assertThat(decoded.decoded().get("paid").asBoolean()).isTrue();
        assertThat(decoded.subject()).isEqualTo(SUBJECT);
    }

    @Test
    void encodeProducesConfluentWireBytesThatDecodeBack() throws Exception {
        var payload = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree("{\"order_id\":\"ENC-1\",\"amount\":55.25,\"paid\":true}");

        var encoded = registryService.encode(KafkaClusterManager.DEFAULT_CLUSTER_ID, SUBJECT,
                null, payload, false, false);

        assertThat(encoded.schemaType()).isEqualTo("PROTOBUF");
        byte[] wire = java.util.Base64.getDecoder().decode(encoded.valueBase64());
        assertThat(wire[0]).isEqualTo((byte) 0x00);
        assertThat(wire[1]).isEqualTo((byte) 0); assertThat(wire[2]).isEqualTo((byte) 0);
        assertThat(wire[3]).isEqualTo((byte) 0); assertThat(wire[4]).isEqualTo((byte) SCHEMA_ID);
        assertThat(wire[5]).isEqualTo((byte) 0x01); // index prefix: top-level message 0, 1-based
        assertThat(wire[6]).isEqualTo((byte) 0x00); // terminator

        // round trip through the decoder
        var decoded = registryService.decode(KafkaClusterManager.DEFAULT_CLUSTER_ID, wire, TOPIC, false);
        assertThat(decoded.decoded().get("order_id").asText()).isEqualTo("ENC-1");
        assertThat(decoded.decoded().get("amount").asDouble()).isEqualTo(55.25);
    }

    @Test
    void encodedMessagesSurviveProduceAndBrowse() throws Exception {
        var payload = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree("{\"order_id\":\"PB-1\",\"amount\":10.5,\"paid\":false}");
        var encoded = registryService.encode(KafkaClusterManager.DEFAULT_CLUSTER_ID, SUBJECT,
                null, payload, false, false);

        ClusterHandle handle = clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID);
        handle.template().send(new org.apache.kafka.clients.producer.ProducerRecord<>(TOPIC,
                "pb-1".getBytes(), java.util.Base64.getDecoder().decode(encoded.valueBase64()))).get();

        var result = new MessageBrowserService(registryService).browse(handle, TOPIC,
                new MessageBrowserService.BrowseRequest(null, "earliest", null, null, 10, 10_000L, null, null));
        assertThat(result.messages()).hasSize(1);
        assertThat(result.messages().get(0).schema().decoded().get("order_id").asText()).isEqualTo("PB-1");
        assertThat(result.messages().get(0).schema().schemaId()).isEqualTo((long) SCHEMA_ID);
    }

    @Test
    void unknownJsonFieldsAreRejected() throws Exception {
        var payload = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree("{\"totally_unknown\":1}");
        assertThatThrownBy(() -> registryService.encode(KafkaClusterManager.DEFAULT_CLUSTER_ID, SUBJECT,
                null, payload, false, false))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("totally_unknown");
    }

    @Test
    void unparseableMessageIsReportedNotThrown() {
        byte[] wire = new byte[]{0x00, 0x00, 0x00, 0x00, 0x07, 0x01, 0x00, 0x00, 0x00};
        var result = registryService.decode(KafkaClusterManager.DEFAULT_CLUSTER_ID, wire, TOPIC, false);
        assertThat(result.decoded()).isNull();
        assertThat(result.error()).isNotNull();
    }
}
