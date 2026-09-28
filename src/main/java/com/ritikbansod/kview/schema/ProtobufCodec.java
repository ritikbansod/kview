package com.ritikbansod.kview.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.TypeElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Protobuf codec for Confluent wire-format payloads. After the 5-byte header a
 * Confluent protobuf payload carries a message-index prefix — 1-based varints
 * terminated by a 0x00 byte, selecting the message within the .proto file —
 * followed by the protobuf message bytes. This class strips that prefix,
 * parses the message against the .proto compiled from the registry, and
 * converts to/from the proto3 JSON mapping (original field names preserved).
 */
public final class ProtobufCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ProtobufCodec() { }

    /** A compiled .proto: the default message name, the top-level messages in declaration order, the schema. */
    public record Compiled(String defaultName, List<String> topLevelMessages, DynamicSchema schema) { }

    public static Compiled compile(String protoText) {
        ProtobufSchema schema = new ProtobufSchema(protoText);
        List<String> topLevel = new ArrayList<>();
        for (TypeElement type : schema.rawSchema().getTypes()) {
            if (type instanceof MessageElement message) {
                topLevel.add(message.getName());
            }
        }
        return new Compiled(schema.name(), List.copyOf(topLevel), schema.toDynamicSchema());
    }

    /**
     * Reads the Confluent message-index prefix and parses the message.
     * Falls back to treating the whole remainder as the message body when the
     * prefix parse does not yield a decodable message (non-Confluent producers).
     */
    public static DynamicMessage decodeMessage(Compiled compiled, byte[] payload) throws Exception {
        Indexes indexes = readIndexes(payload);
        String name = resolveName(compiled, indexes.indexes());
        try {
            return compiled.schema().newMessageBuilder(name)
                    .mergeFrom(payload, indexes.payloadOffset(), payload.length - indexes.payloadOffset())
                    .build();
        } catch (Exception prefixFailure) {
            if (indexes.payloadOffset() == 5 || indexes.indexes().isEmpty()) throw prefixFailure;
            // some producers omit the index array entirely — retry the raw payload
            return compiled.schema().newMessageBuilder(name)
                    .mergeFrom(payload, 5, payload.length - 5)
                    .build();
        }
    }

    /** A message encoded for the wire: the body (index prefix + protobuf bytes) and its JSON render. */
    public record EncodedMessage(byte[] body, JsonNode json) { }

    /** Parses a JSON payload and encodes it with the Confluent index prefix for its message. */
    public static EncodedMessage encode(Compiled compiled, String messageName, JsonNode payload) throws Exception {
        String name = messageName != null && !messageName.isBlank()
                ? messageName : resolveDefaultName(compiled);
        int index = compiled.topLevelMessages().indexOf(name);
        if (index < 0) {
            throw new IllegalArgumentException("messageName '" + name
                    + "' is not a top-level message in this .proto (found: " + compiled.topLevelMessages() + ")");
        }
        DynamicMessage.Builder builder = compiled.schema().newMessageBuilder(name);
        JsonFormat.parser().merge(payload.toString(), builder);
        DynamicMessage message = builder.build();

        byte[] prefix = indexPrefix(index);
        byte[] messageBytes = message.toByteArray();
        byte[] body = new byte[prefix.length + messageBytes.length];
        System.arraycopy(prefix, 0, body, 0, prefix.length);
        System.arraycopy(messageBytes, 0, body, prefix.length, messageBytes.length);
        return new EncodedMessage(body, toJson(message));
    }

    /** Index prefix for a top-level message (Confluent writes 1-based varints, terminated by 0x00). */
    public static byte[] indexPrefix(int messageIndex) {
        if (messageIndex < 0) throw new IllegalArgumentException("Negative message index");
        List<Byte> bytes = new ArrayList<>();
        writeVarInt(bytes, messageIndex + 1);
        bytes.add((byte) 0x00);
        byte[] out = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) out[i] = bytes.get(i);
        return out;
    }

    /** Proto3 JSON rendering with original field names (and defaults made explicit). */
    public static JsonNode toJson(DynamicMessage message) throws Exception {
        String json = JsonFormat.printer()
                .preservingProtoFieldNames()
                .includingDefaultValueFields()
                .print(message);
        return MAPPER.readTree(json);
    }

    static String resolveDefaultName(Compiled compiled) {
        if (compiled.defaultName() != null && !compiled.defaultName().isBlank()
                && compiled.topLevelMessages().contains(compiled.defaultName())) {
            return compiled.defaultName();
        }
        if (compiled.topLevelMessages().size() == 1) {
            return compiled.topLevelMessages().get(0);
        }
        throw new IllegalArgumentException("The .proto defines multiple messages (" +
                compiled.topLevelMessages() + ") — specify which one with messageName");
    }

    private static String resolveName(Compiled compiled, List<Integer> indexes) {
        if (indexes.isEmpty()) {
            return resolveDefaultName(compiled);
        }
        if (indexes.size() > 1) {
            throw new IllegalArgumentException(
                    "Nested protobuf message indexes are not supported (indexes " + indexes + ")");
        }
        int index = indexes.get(0);
        if (index < 0 || index >= compiled.topLevelMessages().size()) {
            throw new IllegalArgumentException("Message index " + index
                    + " is outside the .proto file (" + compiled.topLevelMessages() + ")");
        }
        return compiled.topLevelMessages().get(index);
    }

    record Indexes(List<Integer> indexes, int payloadOffset) { }

    /** Reads 1-based varints terminated by 0x00; an empty list means no index prefix was present. */
    private static Indexes readIndexes(byte[] payload) {
        List<Integer> indexes = new ArrayList<>();
        int pos = 5; // right after the Confluent header
        while (pos < payload.length) {
            int[] read = readVarInt(payload, pos);
            int value = read[0];
            pos = read[1];
            if (value == 0) {
                return new Indexes(indexes, pos); // terminator
            }
            indexes.add(value - 1);
            if (indexes.size() > 64) {
                throw new IllegalArgumentException("Unreasonable protobuf message index prefix (runaway varints)");
            }
        }
        // no terminator found — no index prefix; the payload is the message itself
        return new Indexes(List.of(), 5);
    }

    /** Unsigned LEB128 varint: returns {value, nextPosition}. */
    private static int[] readVarInt(byte[] bytes, int pos) {
        int value = 0, shift = 0, i = pos;
        while (i < bytes.length) {
            int b = bytes[i] & 0xFF;
            value |= (b & 0x7F) << shift;
            i++;
            if ((b & 0x80) == 0) return new int[]{value, i};
            shift += 7;
            if (shift > 35) throw new IllegalArgumentException("Malformed protobuf index varint");
        }
        throw new IllegalArgumentException("Truncated protobuf index varint");
    }

    private static void writeVarInt(List<Byte> out, int value) {
        while (true) {
            int lower = value & 0x7F;
            value >>>= 7;
            if (value == 0) {
                out.add((byte) lower);
                return;
            }
            out.add((byte) (lower | 0x80));
        }
    }
}
