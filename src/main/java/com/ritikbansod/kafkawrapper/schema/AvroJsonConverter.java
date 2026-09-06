package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Converts decoded Avro data ({@link GenericRecord} trees) to Jackson JSON with
 * human-friendly logical types (date, time, timestamp-millis/micros, decimal, uuid).
 * No codegen — works with any writer schema at runtime.
 */
public final class AvroJsonConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Conversions.DecimalConversion DECIMAL = new Conversions.DecimalConversion();

    private AvroJsonConverter() {
    }

    public static JsonNode toJson(Object datum) {
        return convert(datum, null, new IdentityHashMap<>());
    }

    /** Schema-aware conversion: applies logical types (date/time/decimal/uuid). */
    public static JsonNode toJson(Object datum, Schema schema) {
        return convert(datum, schema, new IdentityHashMap<>());
    }

    private static JsonNode convert(Object datum, Schema schema, IdentityHashMap<Object, JsonNode> seen) {
        if (datum == null) return JsonNodeFactory.instance.nullNode();
        if (schema != null) {
            JsonNode viaSchema = convertWithSchema(datum, schema, seen);
            if (viaSchema != null) return viaSchema;
        }
        // generic fallback (schema unknown)
        if (datum instanceof Utf8 u) return JsonNodeFactory.instance.textNode(u.toString());
        if (datum instanceof String s) return JsonNodeFactory.instance.textNode(s);
        if (datum instanceof Boolean b) return JsonNodeFactory.instance.booleanNode(b);
        if (datum instanceof Integer i) return JsonNodeFactory.instance.numberNode(i);
        if (datum instanceof Long l) return JsonNodeFactory.instance.numberNode(l);
        if (datum instanceof Float f) return JsonNodeFactory.instance.numberNode(f.doubleValue());
        if (datum instanceof Double d) return JsonNodeFactory.instance.numberNode(d);
        if (datum instanceof ByteBuffer buf) {
            return JsonNodeFactory.instance.textNode(Base64.getEncoder().encodeToString(buf.array()));
        }
        if (datum instanceof byte[] arr) {
            return JsonNodeFactory.instance.textNode(Base64.getEncoder().encodeToString(arr));
        }
        if (datum instanceof GenericFixed fixed) {
            return JsonNodeFactory.instance.textNode(Base64.getEncoder().encodeToString(fixed.bytes()));
        }
        if (datum instanceof GenericEnumSymbol) {
            return JsonNodeFactory.instance.textNode(datum.toString());
        }
        if (datum instanceof Map<?, ?> map) {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            map.forEach((k, v) -> node.set(String.valueOf(k), convert(v, null, seen)));
            return node;
        }
        if (datum instanceof Iterable<?> list) {
            ArrayNode node = JsonNodeFactory.instance.arrayNode();
            list.forEach(v -> node.add(convert(v, null, seen)));
            return node;
        }
        if (datum instanceof GenericRecord record) {
            if (seen.putIfAbsent(record, JsonNodeFactory.instance.textNode("…")) != null) {
                return JsonNodeFactory.instance.textNode("(recursive)");
            }
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            for (Schema.Field field : record.getSchema().getFields()) {
                node.set(field.name(), convert(record.get(field.name()), field.schema(), seen));
            }
            seen.remove(record);
            return node;
        }
        if (datum instanceof LocalTime t) return JsonNodeFactory.instance.textNode(t.toString());
        if (datum instanceof LocalDate d) return JsonNodeFactory.instance.textNode(d.toString());
        if (datum instanceof LocalDateTime ldt) return JsonNodeFactory.instance.textNode(ldt.toString());
        if (datum instanceof Instant in) return JsonNodeFactory.instance.textNode(in.toString());
        if (datum instanceof BigDecimal bd) return JsonNodeFactory.instance.numberNode(bd);
        return JsonNodeFactory.instance.textNode(String.valueOf(datum));
    }

    /** Applies logical types by inspecting the schema branch that matches the datum. */
    private static JsonNode convertWithSchema(Object datum, Schema schema, IdentityHashMap<Object, JsonNode> seen) {
        if (schema.isUnion()) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() == org.apache.avro.Schema.Type.NULL) continue;
                JsonNode node = convertWithSchema(datum, branch, seen);
                if (node != null) return node;
            }
            return convert(datum, null, seen);
        }
        LogicalType logical = schema.getLogicalType();
        if (logical != null) {
            switch (logical.getName()) {
                case "timestamp-millis":
                    if (datum instanceof Long l) return JsonNodeFactory.instance.textNode(Instant.ofEpochMilli(l).toString());
                    break;
                case "timestamp-micros":
                    if (datum instanceof Long l) return JsonNodeFactory.instance.textNode(
                            Instant.ofEpochSecond(Math.floorDiv(l, 1_000_000L), Math.floorMod(l, 1_000_000L) * 1000L).toString());
                    break;
                case "local-timestamp-millis":
                    if (datum instanceof Long l) return JsonNodeFactory.instance.textNode(
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC).toString());
                    break;
                case "local-timestamp-micros":
                    if (datum instanceof Long l) return JsonNodeFactory.instance.textNode(
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochSecond(Math.floorDiv(l, 1_000_000L), Math.floorMod(l, 1_000_000L) * 1000L),
                                    ZoneOffset.UTC).toString());
                    break;
                case "date":
                    if (datum instanceof Integer i) return JsonNodeFactory.instance.textNode(LocalDate.ofEpochDay(i).toString());
                    break;
                case "time-millis":
                    if (datum instanceof Integer i) return JsonNodeFactory.instance.textNode(
                            LocalTime.ofNanoOfDay(i * 1_000_000L).toString());
                    break;
                case "time-micros":
                    if (datum instanceof Long l) return JsonNodeFactory.instance.textNode(
                            LocalTime.ofNanoOfDay(l * 1000L).toString());
                    break;
                case "decimal":
                    if (datum instanceof ByteBuffer buf) {
                        return JsonNodeFactory.instance.numberNode(DECIMAL.fromBytes(buf, schema, logical));
                    }
                    if (datum instanceof GenericFixed fixed) {
                        return JsonNodeFactory.instance.numberNode(
                                DECIMAL.fromBytes(java.nio.ByteBuffer.wrap(fixed.bytes()), schema, logical));
                    }
                    break;
                case "uuid":
                    if (datum instanceof CharSequence cs) return JsonNodeFactory.instance.textNode(cs.toString());
                    break;
                default:
                    break;
            }
        }
        switch (schema.getType()) {
            case RECORD: {
                if (datum instanceof GenericRecord record) {
                    if (seen.putIfAbsent(record, JsonNodeFactory.instance.textNode("…")) != null) {
                        return JsonNodeFactory.instance.textNode("(recursive)");
                    }
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    for (Schema.Field field : schema.getFields()) {
                        node.set(field.name(), convert(record.get(field.name()), field.schema(), seen));
                    }
                    seen.remove(record);
                    return node;
                }
                break;
            }
            case ARRAY: {
                if (datum instanceof Iterable<?> list) {
                    ArrayNode node = JsonNodeFactory.instance.arrayNode();
                    list.forEach(v -> node.add(convert(v, schema.getElementType(), seen)));
                    return node;
                }
                break;
            }
            case MAP: {
                if (datum instanceof Map<?, ?> map) {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    map.forEach((k, v) -> node.set(String.valueOf(k), convert(v, schema.getValueType(), seen)));
                    return node;
                }
                break;
            }
            case ENUM: {
                if (datum != null) return JsonNodeFactory.instance.textNode(datum.toString());
                break;
            }
            case FIXED: {
                if (datum instanceof GenericFixed fixed) {
                    return JsonNodeFactory.instance.textNode(Base64.getEncoder().encodeToString(fixed.bytes()));
                }
                break;
            }
            default:
                break;
        }
        return null; // caller falls back to generic conversion
    }

    /** Convenience for raw JSON strings. */
    public static JsonNode fromJson(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            return JsonNodeFactory.instance.textNode(json);
        }
    }
}
