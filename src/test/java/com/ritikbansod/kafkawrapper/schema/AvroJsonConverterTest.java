package com.ritikbansod.kafkawrapper.schema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AvroJsonConverterTest {

    private static final String ORDER_SCHEMA = """
            {
              "type": "record",
              "name": "Order",
              "fields": [
                {"name": "orderId", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "items", "type": {"type": "array", "items": "string"}},
                {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
                {"name": "createdAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "note", "type": ["null", "string"], "default": null}
              ]
            }""";

    @Test
    void convertsRecordTreeToJson() throws Exception {
        Schema schema = new Schema.Parser().parse(ORDER_SCHEMA);
        GenericRecord record = new GenericData.Record(schema);
        record.put("orderId", new Utf8("ORD-1"));
        record.put("amount", 42.5);
        record.put("items", new GenericData.Array<>(schema.getField("items").schema(),
                java.util.List.of(new Utf8("espresso"))));
        record.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "NEW"));
        record.put("createdAt", 1699000000000L);
        record.put("note", null);

        var json = AvroJsonConverter.toJson(record, schema);

        assertThat(json.get("orderId").asText()).isEqualTo("ORD-1");
        assertThat(json.get("amount").asDouble()).isEqualTo(42.5);
        assertThat(json.get("items").isArray()).isTrue();
        assertThat(json.get("items").get(0).asText()).isEqualTo("espresso");
        assertThat(json.get("status").asText()).isEqualTo("NEW");
        assertThat(json.get("createdAt").asText())
                .isEqualTo(Instant.ofEpochMilli(1699000000000L).toString());
        assertThat(json.get("note").isNull()).isTrue();
    }
}
