package com.ritikbansod.kafkawrapper.schema;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Conservative wire-format detection for schema-registry encoded payloads.
 * When in doubt the message is reported as UNKNOWN — never guessed.
 */
public final class WireFormatSniffer {

    public enum Format { CONFLUENT, UNKNOWN }

    /** A resolved wire-format header. */
    public record WireHeader(Format format, long schemaId) { }

    private WireFormatSniffer() {
    }

    /**
     * Detects a wire-format header in the raw payload.
     * Confluent-compatible layout: 0x00 + 4-byte big-endian schema ID (5 bytes minimum).
     */
    public static Optional<WireHeader> sniff(byte[] bytes) {
        if (bytes == null || bytes.length < 5 || bytes[0] != 0x00) {
            return Optional.empty();
        }
        long id = ((long) (bytes[1] & 0xFF) << 24)
                | ((long) (bytes[2] & 0xFF) << 16)
                | ((long) (bytes[3] & 0xFF) << 8)
                | (long) (bytes[4] & 0xFF);
        if (id <= 0 || id > Integer.MAX_VALUE) {
            return Optional.empty();
        }
        return Optional.of(new WireHeader(Format.CONFLUENT, id));
    }

    /** Basic view helpers shared by the decode pipeline. */
    public static Map<String, Object> byteViews(byte[] bytes) {
        Map<String, Object> view = new LinkedHashMap<>();
        if (bytes == null) {
            view.put("value", null);
            view.put("valueBase64", null);
            view.put("binary", false);
            return view;
        }
        view.put("value", new String(bytes, StandardCharsets.UTF_8));
        view.put("valueBase64", Base64.getEncoder().encodeToString(bytes));
        view.put("binary", !isStrictUtf8(bytes));
        return view;
    }

    private static boolean isStrictUtf8(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
