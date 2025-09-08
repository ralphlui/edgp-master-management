package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.math.BigDecimal;
import java.time.*;


@Component
public class CSVParser {

    /** Entry point: parse a MultipartFile into JSON-native rows. */
    public static List<LinkedHashMap<String, Object>> parseCsvObjects(MultipartFile file) throws IOException {
        List<LinkedHashMap<String, Object>> result = new ArrayList<>();
        if (file == null || file.isEmpty()) return result;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {

            String headerLine = reader.readLine();
            if (headerLine == null) return result;

            // Strip BOM if present
            if (!headerLine.isEmpty() && headerLine.charAt(0) == '\uFEFF') {
                headerLine = headerLine.substring(1);
            }

            List<String> rawHeaders = parseCsvLine(headerLine);
            List<String> headers = ensureUniqueHeaders(rawHeaders.stream()
                    .map(CSVParser::normalizeHeader)
                    .toList());

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;

                List<String> values = parseCsvLine(line);
                LinkedHashMap<String, Object> row = new LinkedHashMap<>();

                for (int i = 0; i < headers.size(); i++) {
                    String key = headers.get(i);
                    String raw = i < values.size() ? unquote(values.get(i)).trim() : "";
                    Object casted = smartCast(raw);
                    if (key != null && !key.isEmpty()) {
                        row.put(key, casted);
                    }
                }
                result.add(row);
            }
        }
        return result;
    }

    /* ---------------- CSV line parsing ---------------- */

    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"'); // escaped quote
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        fields.add(sb.toString());
        return fields;
    }

    private static String unquote(String s) {
        if (s == null) return null;
        String t = s;
        if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
            t = t.substring(1, t.length() - 1);
        }
        return t.replace("\"\"", "\"");
    }

   
    private static String normalizeHeader(String h) {
        if (h == null) return "";
        String t = h.trim();
        // normalize delimiters/spaces and remove unsafe chars
        t = t.replace(' ', '_').replaceAll("[^A-Za-z0-9_:-]", "_");
        if (t.isEmpty()) t = "col";
        return (t.length() <= 255) ? t : t.substring(0, 255);
    }

    private static List<String> ensureUniqueHeaders(List<String> headers) {
        Map<String, Integer> seen = new HashMap<>();
        List<String> out = new ArrayList<>(headers.size());
        for (String h : headers) {
            int count = seen.getOrDefault(h, 0);
            if (count == 0) out.add(h);
            else out.add(h + "_" + count);
            seen.put(h, count + 1);
        }
        return out;
    }

    private static Object smartCast(String v) {
        if (v == null) return null;
        String s = v.trim();
       
        if (s.isEmpty()) return "";

        // boolean
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(s);
        }

        // number (integer/decimal/scientific, with optional sign)
        if (s.matches("^[+-]?\\d*(?:\\.\\d+)?(?:[eE][+-]?\\d+)?$")) {
            try {
                return new BigDecimal(s);
            } catch (NumberFormatException ignore) {}
        }

        // date/time (normalize to ISO-8601 UTC string if recognized)
        String iso = tryIsoNormalize(s);
        if (iso != null) return iso;

        // default: string
        return s;
    }

    private static String tryIsoNormalize(String s) {
        try { return Instant.parse(s).toString(); } catch (Exception ignored) {}
        try { return OffsetDateTime.parse(s).toInstant().toString(); } catch (Exception ignored) {}
        try { return ZonedDateTime.parse(s).toInstant().toString(); } catch (Exception ignored) {}
        try {
            // Assume Asia/Singapore for naive LocalDateTime, convert to UTC
            LocalDateTime ldt = LocalDateTime.parse(s);
            ZoneId sgt = ZoneId.of("Asia/Singapore");
            return ldt.atZone(sgt).toInstant().toString();
        } catch (Exception ignored) {}
        try { return LocalDate.parse(s).toString(); } catch (Exception ignored) {}
        return null;
    }
}

