package com.ritikbansod.kafkawrapper.cli;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct-mode CLI ({@code --cli}): CliMain boots its own headless context
 * against the embedded broker; commands run end to end without the web server.
 */
@SpringBootTest(properties = "kview.data-dir=target/cli-outer-data")
@EmbeddedKafka(partitions = 1, topics = "cli-test-topic")
class CliModeTest {

    @Autowired
    KafkaClusterManager clusterManager;

    @DynamicPropertySource
    static void kafkaBootstrap(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void directModeProducesBrowsesAndListsWithoutTheServer() throws Exception {
        var template = clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID).template();
        template.send("cli-test-topic", "k-1".getBytes(), "{\"n\":1}".getBytes()).get();
        template.send("cli-test-topic", "k-2".getBytes(), "{\"n\":2}".getBytes()).get();

        // produce through the direct CLI
        var produceOut = new ByteArrayOutputStream();
        var produceErr = new ByteArrayOutputStream();
        int code = CliMain.run(cliArgs("produce", "cli-test-topic", "--key", "k-3", "--value", "{\"n\":3}"),
                new PrintStream(produceOut), new PrintStream(produceErr));
        assertThat(code).as("stderr: %s", produceErr).isZero();
        assertThat(produceOut.toString()).contains("produced →");

        // browse it back, schema section included, json output
        var browseOut = new ByteArrayOutputStream();
        var browseErr = new ByteArrayOutputStream();
        code = CliMain.run(cliArgs("browse", "cli-test-topic", "--start", "earliest", "--limit", "10", "--json"),
                new PrintStream(browseOut), new PrintStream(browseErr));
        assertThat(code).as("stderr: %s", browseErr).isZero();
        assertThat(browseOut.toString()).contains("k-3");
        assertThat(browseOut.toString()).contains("cli-test-topic");
    }

    @Test
    void topicsListsThroughTheDirectConnection() throws Exception {
        var template = clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID).template();
        template.send("cli-test-topic", "k".getBytes(), "{\"n\":0}".getBytes()).get();

        var out = new ByteArrayOutputStream();
        int code = CliMain.run(cliArgs("topics", "--json"), new PrintStream(out), new PrintStream(new ByteArrayOutputStream()));
        assertThat(code).isZero();
        assertThat(out.toString()).contains("cli-test-topic");
    }

    @Test
    void exitCodesSignalUsageErrors() {
        assertThat(CliMain.run(new String[]{}, System.out, System.err)).isEqualTo(2);
        assertThat(CliMain.run(new String[]{"bogus-command", "--bootstrap-server", broker()},
                System.out, System.err)).isEqualTo(2);
        assertThat(CliMain.run(new String[]{"replay", "some-topic", "--bootstrap-server", broker()},
                System.out, System.err)).isEqualTo(1); // missing --to-topic
    }

    private String broker() {
        return System.getProperty("spring.embedded.kafka.brokers");
    }

    private String[] cliArgs(String... rest) {
        List<String> all = new ArrayList<>(List.of(rest));
        all.addAll(List.of("--bootstrap-server", broker(), "--data-dir", "target/cli-test-data"));
        return all.toArray(new String[0]);
    }
}
