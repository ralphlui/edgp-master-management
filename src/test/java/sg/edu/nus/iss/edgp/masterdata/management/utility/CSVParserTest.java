package sg.edu.nus.iss.edgp.masterdata.management.utility;


import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;

class CSVParserTest {

    private MockMultipartFile csv(String name, String content) {
        return new MockMultipartFile("file", name, "text/csv", content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void parseCsvObjects_happyPath_castsAndNormalizesHeaders_andStripsBOM() throws Exception {
       
        String header =
                "\uFEFFFull Name,dup,dup,Active,Balance,LocalDT,OffsetDT,Instant,DateOnly,Note,Special!Header";
       
        String row1 =
                "\"Alice Tan\",D1,D2,true,123.45,2025-01-01T12:00:00,2025-01-01T12:00:00+08:00,2025-01-01T12:00:00Z,2025-01-01,\"He said \"\"Hello\"\"\",xyz";
       
        String row2 =
                "\"Bob\",,,false,-3.5e2,2025-06-30T00:00:00,2025-06-30T00:00:00+08:00,2025-06-29T16:00:00Z,2025-06-30,\"\",@";
       
        String content = header + "\n" + row1 + "\n" + "\n" + row2 + "\n";

        MockMultipartFile file = csv("data.csv", content);
        List<LinkedHashMap<String, Object>> rows = CSVParser.parseCsvObjects(file);

        assertEquals(2, rows.size());
        LinkedHashMap<String, Object> r1 = rows.get(0);
        assertTrue(r1.containsKey("Full_Name"));
        assertTrue(r1.containsKey("dup"));
        assertTrue(r1.containsKey("dup_1"));
        assertTrue(r1.containsKey("Special_Header"));

        assertEquals("Alice Tan", r1.get("Full_Name"));
        assertEquals("D1", r1.get("dup"));
        assertEquals("D2", r1.get("dup_1"));
        assertEquals(Boolean.TRUE, r1.get("Active"));
        assertEquals(new BigDecimal("123.45"), r1.get("Balance"));

       
        String expectedLocalToUtc = LocalDateTime.parse("2025-01-01T12:00:00")
                .atZone(ZoneId.of("Asia/Singapore")).toInstant().toString();
        assertEquals(expectedLocalToUtc, r1.get("LocalDT"));

        String expectedOffsetToUtc = OffsetDateTime.parse("2025-01-01T12:00:00+08:00")
                .toInstant().toString();
        assertEquals(expectedOffsetToUtc, r1.get("OffsetDT"));

        assertEquals("2025-01-01T12:00:00Z", r1.get("Instant"));

        assertEquals("2025-01-01", r1.get("DateOnly"));

        assertEquals("He said \"Hello\"", r1.get("Note"));

        assertEquals("xyz", r1.get("Special_Header"));

        LinkedHashMap<String, Object> r2 = rows.get(1);
        assertEquals("Bob", r2.get("Full_Name"));
       
        assertEquals("", r2.get("dup"));
        assertEquals("", r2.get("dup_1"));
        assertEquals(Boolean.FALSE, r2.get("Active"));
        assertEquals("-350", ((BigDecimal) r2.get("Balance")).toPlainString());


        String expectedLocalToUtc2 = LocalDateTime.parse("2025-06-30T00:00:00")
                .atZone(ZoneId.of("Asia/Singapore")).toInstant().toString();
        assertEquals(expectedLocalToUtc2, r2.get("LocalDT"));

        String expectedOffsetToUtc2 = OffsetDateTime.parse("2025-06-30T00:00:00+08:00")
                .toInstant().toString();
        assertEquals(expectedOffsetToUtc2, r2.get("OffsetDT"));

        assertEquals("2025-06-29T16:00:00Z", r2.get("Instant"));
        assertEquals("2025-06-30", r2.get("DateOnly"));
        assertEquals("", r2.get("Note"));
        assertEquals("@", r2.get("Special_Header"));
    }

    @Test
    void parseCsvObjects_handlesQuotedCommas() throws Exception {
        String content = "City,Comment\n\"Singapore, SG\",\"Nice place\"\n";
        List<LinkedHashMap<String, Object>> rows = CSVParser.parseCsvObjects(csv("c.csv", content));
        assertEquals(1, rows.size());
        Map<String, Object> r = rows.get(0);
        assertEquals("Singapore, SG", r.get("City"));
        assertEquals("Nice place", r.get("Comment"));
    }

    @Test
    void parseCsvObjects_emptyFile_returnsEmptyList() throws Exception {
       
        MockMultipartFile file = new MockMultipartFile("file", "empty.csv", "text/csv", new byte[0]);
        List<LinkedHashMap<String, Object>> rows = CSVParser.parseCsvObjects(file);
        assertTrue(rows.isEmpty());
    }

    @Test
    void parseCsvObjects_nullFile_returnsEmptyList() throws Exception {
        List<LinkedHashMap<String, Object>> rows = CSVParser.parseCsvObjects(null);
        assertTrue(rows.isEmpty());
    }
}
