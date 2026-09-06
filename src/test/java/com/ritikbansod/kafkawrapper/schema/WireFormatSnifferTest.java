package com.ritikbansod.kafkawrapper.schema;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class WireFormatSnifferTest {

    private byte[] confluentPayload(int schemaId, byte[] payload) {
        byte[] out = new byte[5 + payload.length];
        out[0] = 0x00;
        out[1] = (byte) (schemaId >>> 24);
        out[2] = (byte) (schemaId >>> 16);
        out[3] = (byte) (schemaId >>> 8);
        out[4] = (byte) schemaId;
        System.arraycopy(payload, 0, out, 5, payload.length);
        return out;
    }

    @Test
    void detectsConfluentHeader() {
        Optional<WireFormatSniffer.WireHeader> header =
                WireFormatSniffer.sniff(confluentPayload(42, new byte[]{1, 2, 3}));
        assertThat(header).isPresent();
        assertThat(header.get().format()).isEqualTo(WireFormatSniffer.Format.CONFLUENT);
        assertThat(header.get().schemaId()).isEqualTo(42);
    }

    @Test
    void rejectsNonZeroMagic() {
        byte[] json = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        assertThat(WireFormatSniffer.sniff(json)).isEmpty();
    }

    @Test
    void rejectsTooShortOrNullId() {
        assertThat(WireFormatSniffer.sniff(new byte[]{0x00, 0x00, 0x01})).isEmpty();
        assertThat(WireFormatSniffer.sniff(confluentPayload(0, new byte[]{1}))).isEmpty();
        assertThat(WireFormatSniffer.sniff(null)).isEmpty();
    }

    @Test
    void byteViewsFlagBinaryPayloads() {
        var text = WireFormatSniffer.byteViews("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(text.get("value")).isEqualTo("hello");
        assertThat(text.get("binary")).isEqualTo(false);

        var binary = WireFormatSniffer.byteViews(new byte[]{(byte) 0xFF, (byte) 0xFE, 0x01});
        assertThat(text.get("valueBase64")).isNotNull();
        assertThat(binary.get("binary")).isEqualTo(true);
    }
}
