package com.ritikbansod.kafkawrapper.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.util.Base64;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts user-provided JSON into Avro {@link GenericRecord} trees following
 * the writer schema — the encode counterpart of {@link AvroJsonConverter}.
 * Handles unions (null / first-matching branch), enums, field defaults and
 * logical types (date, time, timestamps, decimal, uuid).
 */
public final class JsonToAvroConverter {

    private static final Conversions.DecimalConversion DECIMAL = new Conversions.DecimalConversion();
    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private JsonToAvroConverter() {
    }

    public static GenericRecord fromJson(JsonNode json, Schema schema) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Avro encode expects a record schema, got " + schema.getType());
        }
        return convertRecord(json, schema, new HashMap<>());
    }

    private static GenericRecord convertRecord(JsonNode json, Schema schema, Map<String, Object> seen) {
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            JsonNode value = json == null ? null : json.get(field.name());
            if (value == null || value.isNull()) {
                if (hasNullBranch(field.schema())) {
                    record.put(field.name(), null);
                } else if (field.hasDefaultValue()) {
                    record.put(field.name(), convert(defaultAsJson(field), field.schema(), seen));
                } else {
                    throw new IllegalArgumentException("Missing required field '" + field.name() + "'");
                }
            } else {
                record.put(field.name(), convert(value, field.schema(), seen));
            }
        }
        return record;
    }

    private static boolean hasNullBranch(Schema schema) {
        if (schema.isUnion()) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() == Schema.Type.NULL) return true;
            }
        }
        return false;
    }

    private static JsonNode defaultAsJson(Schema.Field field) {
        Object def = field.defaultVal();
        if (def == null || def == org.apache.avro.Schema.NULL_VALUE) {
            return JsonNull.INSTANCE_VALUE();
        }
        try {
            return MAPPER.valueToTree(def);
        } catch (Exception e) {
            return JsonNull.INSTANCE_VALUE();
        }
    }

    private static Object convert(JsonNode json, Schema schema, Map<String, Object> seen) {
        if (json == null || json.isNull()) return null;

        if (schema.isUnion()) {
            for (Schema branch : schema.getTypes()) {
                if (branch.getType() == Schema.Type.NULL) continue;
                try {
                    return convert(json, branch, seen);
                } catch (Exception branchMismatch) {
                    // try the next union branch
                }
            }
            throw new IllegalArgumentException("Value " + json + " does not match any union branch");
        }

        LogicalType logical = schema.getLogicalType();
        switch (schema.getType()) {
            case RECORD: {
                if (!json.isObject()) {
                    throw new IllegalArgumentException("Expected JSON object for record '"
                            + schema.getName() + "', got: " + json.getNodeType());
                }
                return convertRecord(json, schema, seen);
            }
            case ARRAY: {
                if (!json.isArray()) throw new IllegalArgumentException("Expected array, got: " + json.getNodeType());
                var list = new ArrayList<>();
                for (JsonNode item : json) list.add(convert(item, schema.getElementType(), seen));
                return list;
            }
            case MAP: {
                if (!json.isObject()) throw new IllegalArgumentException("Expected object, got: " + json.getNodeType());
                Map<String, Object> map = new HashMap<>();
                json.fields().forEachRemaining(e ->
                        map.put(e.getKey(), convert(e.getValue(), schema.getValueType(), seen)));
                return map;
            }
            case ENUM:
                return new GenericData.EnumSymbol(schema, json.asText());
            case FIXED: {
                if (logical != null && "decimal".equals(logical.getName())) {
                    int scale = schema.getObjectProp("scale") instanceof Number n ? n.intValue() : 2;
                    BigDecimal decimal = new BigDecimal(json.asText()).setScale(scale, java.math.RoundingMode.HALF_UP);
                    return DECIMAL.toBytes(decimal, schema, logical);
                }
                return new GenericData.Fixed(schema, Base64.getDecoder().decode(json.asText()));
            }
            case STRING: {
                String text = json.isTextual() ? json.textValue() : json.toString();
                if (logical != null && "uuid".equals(logical.getName())) return text;
                return new Utf8(text);
            }
            case BYTES: {
                if (logical != null && "decimal".equals(logical.getName())) {
                    int scale = schema.getObjectProp("scale") instanceof Number n ? n.intValue() : 2;
                    BigDecimal decimal = new BigDecimal(json.asText()).setScale(scale, java.math.RoundingMode.HALF_UP);
                    return DECIMAL.toBytes(decimal, schema, logical);
                }
                return java.nio.ByteBuffer.wrap(Base64.getDecoder().decode(json.asText()));
            }
            case INT: {
                if (logical != null && "date".equals(logical.getName())) {
                    return (int) LocalDate.parse(json.asText()).toEpochDay();
                }
                if (logical != null && "time-millis".equals(logical.getName())) {
                    return (int) (LocalTime.parse(json.asText()).toNanoOfDay() / 1_000_000L);
                }
                return json.asInt();
            }
            case LONG: {
                if (logical != null && "timestamp-millis".equals(logical.getName())) {
                    return Instant.parse(json.asText()).toEpochMilli();
                }
                if (logical != null && "timestamp-micros".equals(logical.getName())) {
                    Instant instant = Instant.parse(json.asText());
                    return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
                }
                if (logical != null && "local-timestamp-millis".equals(logical.getName())) {
                    return LocalDateTime.parse(json.asText().replace("Z", ""))
                            .toInstant(ZoneOffset.UTC).toEpochMilli();
                }
                if (logical != null && "local-timestamp-micros".equals(logical.getName())) {
                    LocalDateTime ldt = LocalDateTime.parse(json.asText().replace("Z", ""));
                    return ldt.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000L
                            + ldt.toInstant(ZoneOffset.UTC).getNano() / 1_000L;
                }
                if (logical != null && "time-micros".equals(logical.getName())) {
                    return LocalTime.parse(json.asText()).toNanoOfDay() / 1_000L;
                }
                return json.asLong();
            }
            case DOUBLE:
                return json.asDouble();
            case FLOAT:
                return (float) json.asDouble();
            case BOOLEAN:
                return json.asBoolean();
            default:
                throw new IllegalArgumentException("Unsupported Avro type " + schema.getType() + " for value " + json);
        }
    }

    /** Tiny sentinel used when a field default is explicitly JSON null. */
    private static final class JsonNull {
        static JsonNode INSTANCE_VALUE() {
            return com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.nullNode();
        }
    }
}
