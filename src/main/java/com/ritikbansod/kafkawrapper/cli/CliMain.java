package com.ritikbansod.kafkawrapper.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ritikbansod.kafkawrapper.KafkaWrapperApplication;
import com.ritikbansod.kafkawrapper.browse.MessageBrowserService;
import com.ritikbansod.kafkawrapper.cluster.ClusterService;
import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import com.ritikbansod.kafkawrapper.connection.ConnectionProfile;
import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import com.ritikbansod.kafkawrapper.connection.SecuritySettings;
import com.ritikbansod.kafkawrapper.group.ConsumerGroupService;
import com.ritikbansod.kafkawrapper.message.ProducerService;
import com.ritikbansod.kafkawrapper.replay.ReplayService;
import com.ritikbansod.kafkawrapper.schema.SchemaRegistryService;
import com.ritikbansod.kafkawrapper.schema.SchemaRegistrySettings;
import com.ritikbansod.kafkawrapper.search.SearchService;
import com.ritikbansod.kafkawrapper.topic.TopicManagementService;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Direct-mode CLI: {@code java -jar kview.jar --cli <command> [options]}.
 *
 * Boots a headless (no web server) application context, registers the
 * connection as an ad-hoc in-memory cluster profile, and reuses the same
 * services as the web UI — so browse and search come out schema-registry
 * decoded, and produce/replay go through the same code paths. Exit code 0 on
 * success, 1 on failure, 2 on usage errors.
 */
public final class CliMain {

    private static final String CLUSTER = "cli";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> BOOLEAN_FLAGS =
            Set.of("json", "dry-run", "no-headers", "include-internal", "help", "no-hostname-verification");

    private CliMain() { }

    public static int run(String[] args) {
        return run(args, System.out, System.err);
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            printHelp(out);
            return args.length == 0 ? 2 : 0;
        }
        String command = args[0];
        Map<String, List<String>> flags = new LinkedHashMap<>();
        List<String> positional = new ArrayList<>();
        positional.add(command); // keep command at positional[0], matching the JS CLI's shape
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--") && arg.length() > 2) {
                String name = arg.substring(2);
                if (BOOLEAN_FLAGS.contains(name)) {
                    flags.computeIfAbsent(name, k -> new ArrayList<>()).add("true");
                } else if (i + 1 < args.length) {
                    flags.computeIfAbsent(name, k -> new ArrayList<>()).add(args[++i]);
                } else {
                    err.println("error: --" + name + " needs a value");
                    return 2;
                }
            } else {
                positional.add(arg);
            }
        }

        try (ConfigurableApplicationContext context = buildContext(flags)) {
            return dispatch(command, flags, positional, context, out, err);
        } catch (CliError e) {
            err.println("error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err.println("error: " + rootMessage(e));
            return 1;
        }
    }

    private static ConfigurableApplicationContext buildContext(Map<String, List<String>> flags) {
        List<String> props = new ArrayList<>();
        props.add("spring.main.web-application-type=none");
        String bootstrap = last(flags, "bootstrap-server");
        if (bootstrap != null) {
            props.add("spring.kafka.bootstrap-servers=" + bootstrap);
        }
        String dataDir = last(flags, "data-dir");
        if (dataDir != null) {
            props.add("kview.data-dir=" + dataDir);
        }
        return new SpringApplicationBuilder(KafkaWrapperApplication.class)
                .web(WebApplicationType.NONE)
                .bannerMode(Banner.Mode.OFF)
                .run(props.toArray(new String[0]));
    }

    private static int dispatch(String command, Map<String, List<String>> flags, List<String> positional,
                                ConfigurableApplicationContext context, PrintStream out, PrintStream err)
            throws Exception {
        KafkaClusterManager manager = context.getBean(KafkaClusterManager.class);
        manager.registerAdHoc(CLUSTER, buildProfile(flags));
        ClusterHandle handle = manager.get(CLUSTER);
        String schemaRegistry = last(flags, "schema-registry");
        if (schemaRegistry != null) {
            context.getBean(SchemaRegistryService.class)
                    .attachVolatile(CLUSTER, SchemaRegistrySettings.confluent(schemaRegistry));
        }
        boolean json = flags.containsKey("json");
        MessageBrowserService browser = context.getBean(MessageBrowserService.class);

        switch (command) {
            case "overview" -> {
                var overview = context.getBean(ClusterService.class).overview(handle);
                if (json) {
                    printJson(out, overview);
                } else {
                    overview.forEach((key, value) -> out.println(key + "  " + value));
                }
            }
            case "topics" -> {
                var topics = context.getBean(TopicManagementService.class).list(handle, true);
                if (!flags.containsKey("include-internal")) {
                    topics = topics.stream().filter(t -> !t.internal()).toList();
                }
                if (json) {
                    printJson(out, topics);
                } else {
                    out.printf("%-40s %11s %4s %10s%n", "TOPIC", "PARTITIONS", "RF", "MESSAGES");
                    topics.forEach(t -> out.printf("%-40s %11d %4d %,10d%n",
                            t.name(), t.partitions(), t.replicationFactor(), t.messageCount()));
                }
            }
            case "topic" -> {
                String topic = positionalOrThrow(positional, "usage: kview topic <name>");
                var detail = context.getBean(TopicManagementService.class).detail(handle, topic);
                if (json) {
                    printJson(out, detail);
                } else {
                    out.println("topic " + detail.name() + "  id=" + detail.topicId());
                    detail.partitions().forEach(p -> out.printf("  p%-3d leader=%-3d replicas=%s isr=%s offsets=%d..%d%n",
                            p.partition(), p.leader(), p.replicas(), p.isr(), p.beginningOffset(), p.endOffset()));
                    detail.configs().forEach(config -> out.println("  " + config.get("name") + "=" + config.get("value")));
                }
            }
            case "browse" -> {
                String topic = positionalOrThrow(positional,
                        "usage: kview browse <topic> [--start earliest|latest|timestamp] [--ts MS|ISO] [--limit N] [--partition N] [--key-contains S] [--value-contains S]");
                var request = new MessageBrowserService.BrowseRequest(
                        intOrNull(flags, "partition"),
                        last(flags, "start") == null ? "latest" : last(flags, "start"),
                        null,
                        tsOf(last(flags, "ts")),
                        intOrNull(flags, "limit") == null ? 50 : intOrNull(flags, "limit"),
                        15_000L,
                        last(flags, "key-contains"),
                        last(flags, "value-contains"));
                var result = browser.browse(handle, topic, request);
                if (json) {
                    printJson(out, result);
                } else {
                    result.messages().forEach(m -> out.println(messageLine(m)));
                    err.println("-- " + result.messages().size() + " message(s) from " + topic
                            + (result.reachedEnd() ? " (end reached)" : " (more may exist — raise --limit)"));
                }
            }
            case "groups" -> {
                var groups = context.getBean(ConsumerGroupService.class).list(handle);
                if (json) {
                    printJson(out, groups);
                } else {
                    out.printf("%-40s %-12s %8s %12s%n", "GROUP", "STATE", "PARTS", "TOTAL LAG");
                    groups.forEach(g -> out.printf("%-40s %-12s %8d %,12d%n",
                            g.groupId(), g.state(), g.committedPartitions(), g.totalLag()));
                }
            }
            case "lag" -> {
                String group = positionalOrThrow(positional, "usage: kview lag <group>");
                var detail = context.getBean(ConsumerGroupService.class).detail(handle, group);
                if (json) {
                    printJson(out, detail);
                } else {
                    out.println("group " + detail.groupId() + "  state=" + detail.state()
                            + "  members=" + detail.members().size() + "  totalLag=" + detail.totalLag());
                    detail.partitionsByTopic().forEach((topic, parts) -> parts.forEach(p ->
                            out.printf("  %s p%-3d committed=%s end=%s lag=%,d%n",
                                    topic, p.partition(), p.committedOffset(), p.endOffset(), p.lag())));
                }
            }
            case "produce" -> {
                String topic = positionalOrThrow(positional,
                        "usage: kview produce <topic> --key K --value V [--header k=v ...] [--partition N]");
                String value = last(flags, "value");
                if (value == null) throw new CliError("--value is required");
                Map<String, String> headers = new LinkedHashMap<>();
                for (String pair : flags.getOrDefault("header", List.of())) {
                    int eq = pair.indexOf('=');
                    if (eq <= 0) throw new CliError("--header must be k=v, got '" + pair + "'");
                    headers.put(pair.substring(0, eq), pair.substring(eq + 1));
                }
                var produced = context.getBean(ProducerService.class).produce(handle, topic,
                        last(flags, "key"), value, null, intOrNull(flags, "partition"), null, headers);
                if (json) {
                    printJson(out, produced);
                } else {
                    out.println("produced → " + produced.get("topic") + " p" + produced.get("partition")
                            + "@" + produced.get("offset"));
                }
            }
            case "search" -> {
                String topic = positionalOrThrow(positional,
                        "usage: kview search <topic> [--value-regex RE] [--key-contains S] [--value-contains S] [--from-ts MS|ISO] [--to-ts MS|ISO] [--limit N] [--partition N] [--wait SEC]");
                SearchService searchService = context.getBean(SearchService.class);
                String searchId = searchService.start(CLUSTER, topic, new SearchService.SearchRequest(
                        tsOf(last(flags, "from-ts")), tsOf(last(flags, "to-ts")),
                        last(flags, "key-contains"), last(flags, "value-contains"), last(flags, "value-regex"),
                        intOrNull(flags, "partition"), intOrNull(flags, "limit")));
                int waitSec = intOrNull(flags, "wait") == null ? 60 : intOrNull(flags, "wait");
                long deadline = System.currentTimeMillis() + waitSec * 1000L;
                var status = searchService.status(CLUSTER, searchId);
                while ("RUNNING".equals(status.state()) && System.currentTimeMillis() < deadline) {
                    Thread.sleep(500);
                    status = searchService.status(CLUSTER, searchId);
                }
                if (json) {
                    printJson(out, status);
                } else {
                    status.results().forEach(m -> out.println(messageLine(m)));
                    err.println("-- " + status.matched() + " matched · scanned " + status.scanned()
                            + " · state " + status.state()
                            + ("RUNNING".equals(status.state())
                                ? " (still running — re-run with --wait " + (waitSec * 2) + ")" : ""));
                }
            }
            case "replay" -> {
                String topic = positionalOrThrow(positional,
                        "usage: kview replay <sourceTopic> --to-topic T [--limit N] [--key-contains S] [--value-contains S] [--dry-run] [--no-headers]");
                String toTopic = last(flags, "to-topic");
                if (toTopic == null) throw new CliError("--to-topic is required");
                var result = context.getBean(ReplayService.class).replay(CLUSTER, topic,
                        new ReplayService.ReplayRequest(null, toTopic, !flags.containsKey("no-headers"),
                                flags.containsKey("dry-run"), intOrNull(flags, "limit"),
                                last(flags, "key-contains"), last(flags, "value-contains")));
                if (json) {
                    printJson(out, result);
                } else {
                    out.println(result.dryRun()
                            ? "dry run: would copy " + result.selected() + " message(s) to " + result.targetTopic()
                            : "replayed " + result.copied() + "/" + result.selected() + " message(s) → "
                              + result.targetTopic()
                              + (result.reachedEnd() ? " (end reached)" : " (more may remain — raise --limit)"));
                    result.errors().forEach(e -> err.println("error: " + e));
                }
            }
            default -> {
                err.println("unknown command: " + command);
                printHelp(err);
                return 2;
            }
        }
        return 0;
    }

    private static ConnectionProfile buildProfile(Map<String, List<String>> flags) {
        List<String> servers = List.of(last(flags, "bootstrap-server") == null
                ? "localhost:9092" : last(flags, "bootstrap-server"));
        SecuritySettings security = new SecuritySettings(
                last(flags, "security-protocol"),
                last(flags, "sasl-mechanism"),
                last(flags, "sasl-username"),
                last(flags, "sasl-password"),
                last(flags, "oauth-token-url"),
                last(flags, "oauth-client-id"),
                last(flags, "oauth-client-secret"),
                last(flags, "oauth-scope"),
                last(flags, "keystore-location"),
                last(flags, "keystore-password"),
                last(flags, "keystore-type"),
                last(flags, "truststore-location"),
                last(flags, "truststore-password"),
                last(flags, "truststore-type"),
                null, null, null,
                flags.containsKey("no-hostname-verification") ? Boolean.FALSE : null);
        return new ConnectionProfile(CLUSTER, "CLI (" + servers.get(0) + ")", servers, security);
    }

    private static String messageLine(MessageBrowserService.BrowserMessage m) {
        String decoded = m.schema() != null && m.schema().decoded() != null
                ? m.schema().decoded().toString()
                : m.value() != null ? m.value() : m.valueBase64() == null ? "(empty)" : "<binary>";
        String value = decoded.length() > 200 ? decoded.substring(0, 200) + "…" : decoded;
        return "[p" + m.partition() + "@" + m.offset() + "] " + Instant.ofEpochMilli(m.timestamp())
                + "  " + (m.key() == null ? "(null)" : m.key()) + "  " + value;
    }

    private static void printJson(PrintStream out, Object value) throws Exception {
        out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value));
    }

    private static String positionalOrThrow(List<String> positional, String usage) {
        if (positional.size() < 2) throw new CliError(usage);
        return positional.get(1);
    }

    private static String last(Map<String, List<String>> flags, String name) {
        List<String> values = flags.get(name);
        return values == null || values.isEmpty() ? null : values.get(values.size() - 1);
    }

    private static Integer intOrNull(Map<String, List<String>> flags, String name) {
        String value = last(flags, name);
        if (value == null) return null;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new CliError("--" + name + " must be a number, got '" + value + "'");
        }
    }

    private static Long tsOf(String value) {
        if (value == null) return null;
        if (value.matches("-?\\d{13,}")) return Long.parseLong(value);
        return java.time.Instant.parse(value).toEpochMilli();
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) t = t.getCause();
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    private static void printHelp(PrintStream out) {
        out.println("""
                kview CLI (direct mode) — talks to a Kafka broker without a server.

                Usage: java -jar kview.jar --cli <command> [args] [options]

                Commands:
                  overview                                cluster KPIs
                  topics [--include-internal]             all topics with counts
                  topic <name>                            partition/config detail
                  browse <topic> [--start earliest|latest|timestamp] [--ts MS|ISO]
                          [--limit N] [--partition N] [--key-contains S] [--value-contains S]
                  groups                                  consumer groups + total lag
                  lag <group>                             per-partition lag
                  produce <topic> --key K --value V [--header k=v ...] [--partition N]
                  search <topic> [--value-regex RE] [--key-contains S] [--value-contains S]
                          [--from-ts MS|ISO] [--to-ts MS|ISO] [--limit N] [--partition N] [--wait SEC]
                  replay <topic> --to-topic T [--limit N] [--key-contains S]
                          [--value-contains S] [--dry-run] [--no-headers]

                Connection options:
                  --bootstrap-server HOST:PORT   (default localhost:9092, env KAFKA_BOOTSTRAP_SERVERS)
                  --security-protocol PLAINTEXT|SSL|SASL_SSL|SASL_PLAINTEXT
                  --sasl-mechanism / --sasl-username / --sasl-password
                  --keystore-location / --keystore-password / --keystore-type
                  --truststore-location / --truststore-password / --truststore-type
                  --oauth-token-url / --oauth-client-id / --oauth-client-secret / --oauth-scope
                  --no-hostname-verification
                  --schema-registry URL          decode schema-registry payloads in browse/search
                  --data-dir DIR                 where Kview state lives (default ./data)
                  --json                         machine-readable output

                Examples:
                  java -jar kview.jar --cli topics --bootstrap-server localhost:9092
                  java -jar kview.jar --cli browse orders --start latest --limit 10
                  java -jar kview.jar --cli search orders --value-regex '"orderId"\\s*:\\s*"ORD-[0-9]+"'
                """);
    }

    static final class CliError extends RuntimeException {
        CliError(String message) {
            super(message);
        }
    }
}
