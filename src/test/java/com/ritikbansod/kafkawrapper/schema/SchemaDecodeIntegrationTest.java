package com.ritikbansod.kafkawrapper.schema;

import com.ritikbansod.kafkawrapper.browse.MessageBrowserService;
import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import com.sun.net.httpserver.HttpServer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 acceptance: a Confluent-compatible registry (mocked over HTTP) is
 * attached to the default cluster; Avro messages produced in the standard
 * wire format decode automatically through the browser.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = SchemaDecodeIntegrationTest.TOPIC)
class SchemaDecodeIntegrationTest {

    static final String TOPIC = "sr-orders";

    private static HttpServer fakeRegistry;
    private static String fakeRegistryUrl;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
        registry.add("kafka-wrapper.data-dir", () -> "target/sr-test-data");
    }

    @Autowired
    MessageBrowserService browserService;

    @Autowired
    KafkaClusterManager clusterManager;


    @Autowired
    SchemaRegistryService registryService;

    private static final String SCHEMA_JSON = """
            {"type":"record","name":"Order","fields":[
              {"name":"orderId","type":"string"},
              {"name":"amount","type":"double"}
            ]}""";

    @BeforeEach
    void startFakeRegistryAndAttach() throws Exception {
        fakeRegistry = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeRegistry.createContext("/schemas/ids/1", exchange -> {
            byte[] body = ("{\"schemaType\":\"AVRO\",\"schema\":" +
                    com.fasterxml.jackson.databind.node.TextNode.valueOf(SCHEMA_JSON).toString() + "}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        fakeRegistry.createContext("/subjects", exchange -> {
            byte[] body = "[\"sr-orders-value\"]".getBytes(StandardCharsets.UTF_8);
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

    /** Avro-encodes the record and prepends the Confluent wire header (0x00 + schema id). */
    private byte[] wirePayload(int schemaId) throws Exception {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("orderId", "ORD-99");
        record.put("amount", 91.5);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x00);
        out.write((schemaId >>> 24) & 0xFF);
        out.write((schemaId >>> 16) & 0xFF);
        out.write((schemaId >>> 8) & 0xFF);
        out.write(schemaId & 0xFF);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    @Test
    void registryAttachedMessagesDecodeAutomatically() throws Exception {
        var handle = clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID);
        assertThat(handle.template().send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                TOPIC, "o-1".getBytes(), wirePayload(1))).get()).isNotNull();

        var result = browserService.browse(handle, TOPIC,
                new MessageBrowserService.BrowseRequest(
                        null, "earliest", null, null, 10, 10_000L, "o-1", null));

        assertThat(result.messages()).hasSize(1);
        var message = result.messages().get(0);

        assertThat(message.schema()).as("decode result: %s", message.schema()).isNotNull();
        assertThat(message.schema().wireFormat())
                .as("decode: %s | note=%s | error=%s", message.schema(), message.schema().note(), message.schema().error())
                .isEqualTo("CONFLUENT");
        assertThat(message.schema().schemaId()).isEqualTo(1L);
        assertThat(message.schema().subject()).isEqualTo("sr-orders-value");
        assertThat(message.schema().decoded().get("orderId").asText()).isEqualTo("ORD-99");
        assertThat(message.schema().decoded().get("amount").asDouble()).isEqualTo(91.5);

        // /decode endpoint works from base64 too
        var decoded = registryService.decode(KafkaClusterManager.DEFAULT_CLUSTER_ID,
                java.util.Base64.getDecoder().decode(message.valueBase64()), TOPIC, false);
        assertThat(decoded.decoded().get("amount").asDouble()).isEqualTo(91.5);
    }

    @Test
    void unattachedFormatIsReportedNotGuessed() {
        // detach, then decode the same wire bytes: wrapper must report UNKNOWN, never guess
        registryService.delete(KafkaClusterManager.DEFAULT_CLUSTER_ID);
        var payload = new byte[]{0x00, 0x00, 0x00, 0x00, 0x07, 0x01, 0x02};
        var result = registryService.decode(KafkaClusterManager.DEFAULT_CLUSTER_ID, payload, TOPIC, false);
        assertThat(result.wireFormat()).isEqualTo("UNKNOWN");
        assertThat(result.decoded()).isNull();
    }
}
