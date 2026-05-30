package io.cqserver.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal, dependency-free JSON parser and serializer.
 *
 * <p>Decoding maps JSON onto plain Java types:
 * <ul>
 *   <li>object -&gt; {@link LinkedHashMap}&lt;String,Object&gt; (insertion ordered)</li>
 *   <li>array  -&gt; {@link ArrayList}&lt;Object&gt;</li>
 *   <li>string -&gt; {@link String}</li>
 *   <li>number -&gt; {@link Long} (no fraction/exponent) or {@link Double}</li>
 *   <li>true/false -&gt; {@link Boolean}, null -&gt; {@code null}</li>
 * </ul>
 *
 * <p>Encoding accepts those same types plus any {@link Number} and {@link CharSequence}.
 * String escaping (quote, backslash, control chars, and unicode escapes) is handled in
 * both directions.
 */
public final class Json {

    private Json() {
    }

    // ---- public API -------------------------------------------------

    /** Parse a JSON document. Returns a Map/List/String/Long/Double/Boolean/null. */
    public static Object parse(String text) {
        Parser p = new Parser(text);
        p.skipWs();
        Object v = p.parseValue();
        p.skipWs();
        if (!p.atEnd()) {
            throw new CqException("trailing characters in JSON at index " + p.pos);
        }
        return v;
    }

    /** Convenience: parse and require a JSON object. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object v = parse(text);
        if (!(v instanceof Map)) {
            throw new CqException("expected JSON object, got " + typeName(v));
        }
        return (Map<String, Object>) v;
    }

    /** Serialize a value to compact JSON (no insignificant whitespace). */
    public static String write(Object value) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, value);
        return sb.toString();
    }

    // ---- serializer -------------------------------------------------

    private static void writeValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof CharSequence) {
            writeString(sb, value.toString());
        } else if (value instanceof Boolean) {
            sb.append(((Boolean) value) ? "true" : "false");
        } else if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new CqException("cannot serialize non-finite number: " + d);
            }
            sb.append(formatDouble(d));
        } else if (value instanceof Number) {
            // Integral types (Long, Integer, Short, Byte, BigInteger, ...).
            sb.append(value.toString());
        } else if (value instanceof Map) {
            writeObject(sb, (Map<?, ?>) value);
        } else if (value instanceof Iterable) {
            writeArray(sb, (Iterable<?>) value);
        } else if (value instanceof Object[]) {
            writeArray(sb, java.util.Arrays.asList((Object[]) value));
        } else if (value instanceof int[]) {
            int[] a = (int[]) value;
            sb.append('[');
            for (int i = 0; i < a.length; i++) {
                if (i > 0) sb.append(',');
                sb.append(a[i]);
            }
            sb.append(']');
        } else if (value instanceof long[]) {
            long[] a = (long[]) value;
            sb.append('[');
            for (int i = 0; i < a.length; i++) {
                if (i > 0) sb.append(',');
                sb.append(a[i]);
            }
            sb.append(']');
        } else {
            throw new CqException("cannot serialize type: " + value.getClass().getName());
        }
    }

    private static void writeObject(StringBuilder sb, Map<?, ?> map) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            Object k = e.getKey();
            if (k == null) {
                throw new CqException("null object key not allowed in JSON");
            }
            writeString(sb, k.toString());
            sb.append(':');
            writeValue(sb, e.getValue());
        }
        sb.append('}');
    }

    private static void writeArray(StringBuilder sb, Iterable<?> items) {
        sb.append('[');
        boolean first = true;
        for (Object item : items) {
            if (!first) sb.append(',');
            first = false;
            writeValue(sb, item);
        }
        sb.append(']');
    }

    private static String formatDouble(double d) {
        // Render integral-valued doubles without a trailing ".0" only if we want
        // to match Python's json; Python keeps "1.0". We keep Double.toString,
        // which also yields "1.0" — consistent and round-trippable.
        return Double.toString(d);
    }

    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append("\\u");
                        for (int shift = 12; shift >= 0; shift -= 4) {
                            sb.append(HEX[(c >> shift) & 0xF]);
                        }
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    // ---- parser -----------------------------------------------------

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        boolean atEnd() {
            return pos >= s.length();
        }

        void skipWs() {
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        private char peek() {
            if (pos >= s.length()) {
                throw new CqException("unexpected end of JSON input");
            }
            return s.charAt(pos);
        }

        Object parseValue() {
            skipWs();
            char c = peek();
            switch (c) {
                case '{':
                    return parseObjectValue();
                case '[':
                    return parseArray();
                case '"':
                    return parseString();
                case 't':
                    return parseLiteral("true", Boolean.TRUE);
                case 'f':
                    return parseLiteral("false", Boolean.FALSE);
                case 'n':
                    return parseLiteral("null", null);
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        return parseNumber();
                    }
                    throw new CqException("unexpected character '" + c + "' at index " + pos);
            }
        }

        private Object parseLiteral(String lit, Object value) {
            if (pos + lit.length() > s.length()
                    || !s.regionMatches(pos, lit, 0, lit.length())) {
                throw new CqException("invalid literal at index " + pos);
            }
            pos += lit.length();
            return value;
        }

        private Map<String, Object> parseObjectValue() {
            Map<String, Object> map = new LinkedHashMap<>();
            pos++; // consume '{'
            skipWs();
            if (peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWs();
                if (peek() != '"') {
                    throw new CqException("expected string key at index " + pos);
                }
                String key = parseString();
                skipWs();
                if (peek() != ':') {
                    throw new CqException("expected ':' at index " + pos);
                }
                pos++; // consume ':'
                Object value = parseValue();
                map.put(key, value);
                skipWs();
                char c = peek();
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == '}') {
                    pos++;
                    return map;
                }
                throw new CqException("expected ',' or '}' at index " + pos);
            }
        }

        private List<Object> parseArray() {
            List<Object> list = new ArrayList<>();
            pos++; // consume '['
            skipWs();
            if (peek() == ']') {
                pos++;
                return list;
            }
            while (true) {
                Object value = parseValue();
                list.add(value);
                skipWs();
                char c = peek();
                if (c == ',') {
                    pos++;
                    continue;
                }
                if (c == ']') {
                    pos++;
                    return list;
                }
                throw new CqException("expected ',' or ']' at index " + pos);
            }
        }

        private String parseString() {
            pos++; // consume opening quote
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (pos >= s.length()) {
                    throw new CqException("unterminated string");
                }
                char c = s.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (pos >= s.length()) {
                        throw new CqException("unterminated escape");
                    }
                    char esc = s.charAt(pos++);
                    switch (esc) {
                        case '"':
                            sb.append('"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case '/':
                            sb.append('/');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'u':
                            sb.append(parseUnicodeEscape());
                            break;
                        default:
                            throw new CqException("invalid escape '\\" + esc + "'");
                    }
                } else if (c < 0x20) {
                    throw new CqException("unescaped control character in string: 0x"
                            + Integer.toHexString(c));
                } else {
                    sb.append(c);
                }
            }
        }

        private char parseUnicodeEscape() {
            if (pos + 4 > s.length()) {
                throw new CqException("truncated \\u escape");
            }
            int code = 0;
            for (int i = 0; i < 4; i++) {
                char h = s.charAt(pos++);
                int d;
                if (h >= '0' && h <= '9') {
                    d = h - '0';
                } else if (h >= 'a' && h <= 'f') {
                    d = h - 'a' + 10;
                } else if (h >= 'A' && h <= 'F') {
                    d = h - 'A' + 10;
                } else {
                    throw new CqException("invalid hex digit '" + h + "' in \\u escape");
                }
                code = (code << 4) | d;
            }
            return (char) code;
        }

        private Object parseNumber() {
            int start = pos;
            boolean isDouble = false;
            if (peek() == '-') {
                pos++;
            }
            while (pos < s.length() && isDigit(s.charAt(pos))) {
                pos++;
            }
            if (pos < s.length() && s.charAt(pos) == '.') {
                isDouble = true;
                pos++;
                while (pos < s.length() && isDigit(s.charAt(pos))) {
                    pos++;
                }
            }
            if (pos < s.length() && (s.charAt(pos) == 'e' || s.charAt(pos) == 'E')) {
                isDouble = true;
                pos++;
                if (pos < s.length() && (s.charAt(pos) == '+' || s.charAt(pos) == '-')) {
                    pos++;
                }
                while (pos < s.length() && isDigit(s.charAt(pos))) {
                    pos++;
                }
            }
            String num = s.substring(start, pos);
            if (num.isEmpty() || "-".equals(num)) {
                throw new CqException("invalid number at index " + start);
            }
            if (isDouble) {
                return Double.parseDouble(num);
            }
            try {
                return Long.parseLong(num);
            } catch (NumberFormatException e) {
                // Out of long range — fall back to double to avoid data loss.
                return Double.parseDouble(num);
            }
        }

        private static boolean isDigit(char c) {
            return c >= '0' && c <= '9';
        }
    }

    private static String typeName(Object v) {
        return v == null ? "null" : v.getClass().getSimpleName();
    }
}
