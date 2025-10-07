package sg.edu.nus.iss.edgp.masterdata.management.utility;


import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.utility.GeneralUtility.BuiltUpdate;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class GeneralUtilityTest {

    private GeneralUtility util;

    @BeforeEach
    void setUp() {
        util = new GeneralUtility();
    }

   

    @Test
    void makeNotNull_behavesAsExpected() {
        assertEquals("", GeneralUtility.makeNotNull(null));
        assertEquals("", GeneralUtility.makeNotNull("null"));
        assertEquals("abc", GeneralUtility.makeNotNull("abc"));
        assertEquals("123", GeneralUtility.makeNotNull(123));
    }

    @Test
    void nowSgt_formatIsCorrect() {
        String ts = GeneralUtility.nowSgt();
        assertNotNull(ts);
        assertTrue(Pattern.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", ts));
    }

    @Test
    void merge_overridesValuesFromSecond() {
        Map<String, String> a = new LinkedHashMap<>(Map.of("k1", "v1", "k2", "v2"));
        Map<String, String> b = new LinkedHashMap<>(Map.of("k2", "X", "k3", "v3"));
        Map<String, String> m = GeneralUtility.merge(a, b);
        assertEquals(3, m.size());
        assertEquals("v1", m.get("k1"));
        assertEquals("X", m.get("k2"));
        assertEquals("v3", m.get("k3"));
    }


    @Test
    void toAttr_handlesNullNumberBoolStringMapList() {
         
        assertEquals(Boolean.TRUE, util.toAttr(null).nul());
 
        assertEquals("123", util.toAttr(123).n());
        assertEquals("3.14", util.toAttr(3.14).n());
        assertEquals("1.23", util.toAttr(new BigDecimal("1.23")).n());

        
        assertTrue(util.toAttr(Boolean.TRUE).bool());
 
        assertEquals("abc", util.toAttr("abc").s());

         
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("x", 9);
        AttributeValue m = util.toAttr(Map.of("k", inner));
        assertTrue(m.hasM());
        assertEquals("9", m.m().get("k").m().get("x").n());

        
        AttributeValue l = util.toAttr(List.of("a", 1, false));
        assertTrue(l.hasL());
        assertEquals("a", l.l().get(0).s());
        assertEquals("1", l.l().get(1).n());
        assertEquals(Boolean.FALSE, l.l().get(2).bool());
    }

     

    @Test
    void equalsAttr_comparesCommonShapes() {
        AttributeValue s1 = AttributeValue.builder().s("hi").build();
        AttributeValue s2 = AttributeValue.builder().s("hi").build();
        AttributeValue s3 = AttributeValue.builder().s("bye").build();
        assertTrue(GeneralUtility.equalsAttr(s1, s2));
        assertFalse(GeneralUtility.equalsAttr(s1, s3));

        AttributeValue n1 = AttributeValue.builder().n("42").build();
        AttributeValue n2 = AttributeValue.builder().n("42").build();
        assertTrue(GeneralUtility.equalsAttr(n1, n2));

        AttributeValue b1 = AttributeValue.builder().bool(true).build();
        AttributeValue b2 = AttributeValue.builder().bool(true).build();
        AttributeValue b3 = AttributeValue.builder().bool(false).build();
        assertTrue(GeneralUtility.equalsAttr(b1, b2));
        assertFalse(GeneralUtility.equalsAttr(b1, b3));

        AttributeValue nul1 = AttributeValue.builder().nul(true).build();
        AttributeValue nul2 = AttributeValue.builder().nul(true).build();
        assertTrue(GeneralUtility.equalsAttr(nul1, nul2));

        AttributeValue m1 = AttributeValue.builder().m(Map.of("k", AttributeValue.builder().n("1").build())).build();
        AttributeValue m2 = AttributeValue.builder().m(Map.of("k", AttributeValue.builder().n("1").build())).build();
        assertTrue(GeneralUtility.equalsAttr(m1, m2));

        AttributeValue l1 = AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().n("2").build()).build();
        AttributeValue l2 = AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().n("2").build()).build();
        assertTrue(GeneralUtility.equalsAttr(l1, l2));

        AttributeValue ss1 = AttributeValue.builder().ss(List.of("x","y")).build();
        AttributeValue ss2 = AttributeValue.builder().ss(List.of("x","y")).build();
        assertTrue(GeneralUtility.equalsAttr(ss1, ss2));

        AttributeValue ns1 = AttributeValue.builder().ns(List.of("1","2")).build();
        AttributeValue ns2 = AttributeValue.builder().ns(List.of("1","2")).build();
        assertTrue(GeneralUtility.equalsAttr(ns1, ns2));
    }

    

    @Test
    void fromAttr_convertsAllSupportedTypes() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("str", AttributeValue.builder().s("abc").build());
        item.put("num", AttributeValue.builder().n("42").build());
        item.put("bool", AttributeValue.builder().bool(true).build());
        item.put("nul", AttributeValue.builder().nul(true).build());
        item.put("obj", AttributeValue.builder().m(Map.of(
                "k", AttributeValue.builder().n("7").build(),
                "s", AttributeValue.builder().s("v").build()
        )).build());
        item.put("list", AttributeValue.builder().l(
                AttributeValue.builder().s("x").build(),
                AttributeValue.builder().n("3").build(),
                AttributeValue.builder().bool(false).build()
        ).build());
        item.put("ss", AttributeValue.builder().ss(List.of("a","b")).build());
        item.put("ns", AttributeValue.builder().ns(List.of("1","2")).build());
        
        SdkBytes b1 = SdkBytes.fromByteBuffer(ByteBuffer.wrap(new byte[]{1,2}));
        SdkBytes b2 = SdkBytes.fromByteBuffer(ByteBuffer.wrap(new byte[]{3}));
        item.put("bs", AttributeValue.builder().bs(b1, b2).build());

        Map<String, Object> out = GeneralUtility.fromAttrMap(item);

        assertEquals("abc", out.get("str"));
        assertEquals(new BigDecimal("42"), out.get("num"));
        assertEquals(Boolean.TRUE, out.get("bool"));
        assertNull(out.get("nul"));

        @SuppressWarnings("unchecked")
        Map<String, Object> obj = (Map<String, Object>) out.get("obj");
        assertEquals(new BigDecimal("7"), obj.get("k"));
        assertEquals("v", obj.get("s"));

        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) out.get("list");
        assertEquals("x", list.get(0));
        assertEquals(new BigDecimal("3"), list.get(1));
        assertEquals(Boolean.FALSE, list.get(2));

        @SuppressWarnings("unchecked")
        Set<String> ss = (Set<String>) out.get("ss");
        assertEquals(Set.of("a","b"), ss);

        @SuppressWarnings("unchecked")
        Set<String> ns = (Set<String>) out.get("ns");
        assertEquals(Set.of("1","2"), ns);

        @SuppressWarnings("unchecked")
        Set<SdkBytes> bs = (Set<SdkBytes>) out.get("bs");
        assertEquals(2, bs.size());
    }

     

    @Test
    void buildStagingUpdateParts_updatesChangedFields_andResetsWorkflowFields() {
         
        Map<String, AttributeValue> current = new LinkedHashMap<>();
        current.put("id", AttributeValue.builder().s("S1").build());
        current.put("name", AttributeValue.builder().s("Alice").build());
        current.put("age", AttributeValue.builder().n("30").build());
        current.put("processed_at", AttributeValue.builder().s("2025-01-01 00:00:00").build());
        current.put("is_processed", AttributeValue.builder().n("1").build());
        current.put("claimed_at", AttributeValue.builder().s("2025-01-01 00:00:00").build());
        current.put("is_handled", AttributeValue.builder().n("1").build());
        current.put("updated_date", AttributeValue.builder().s("2025-01-02 00:00:00").build());

        
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", "S1");
        payload.put("name", "Alice");
        payload.put("age", 31);

        BuiltUpdate bu = util.buildStagingUpdateParts(payload, current);

        
        assertFalse(bu.setParts.isEmpty());
        
        assertEquals(6, bu.setParts.size());
        assertEquals(6, bu.updatedFields);

         
        String joined = String.join(";", bu.setParts);
        assertTrue(joined.contains("= :v0"));
        assertTrue(joined.contains("#processed_at = :processedEmpty"));
        assertTrue(joined.contains("#is_processed = :zeroProcessed"));
        assertTrue(joined.contains("#claimed_at = :claimedEmpty"));
        assertTrue(joined.contains("#is_handled = :zeroHandled"));
        assertTrue(joined.contains("#updated_date = :now"));

        
        assertEquals("31", bu.eav.get(":v0").n());
        assertEquals("", bu.eav.get(":processedEmpty").s());
        assertEquals("0", bu.eav.get(":zeroProcessed").n());
        assertEquals("", bu.eav.get(":claimedEmpty").s());
        assertEquals("0", bu.eav.get(":zeroHandled").n());
        assertNotNull(bu.eav.get(":now").s());
        assertTrue(Pattern.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", bu.eav.get(":now").s()));
    }
 

    @Test
    void buildHeaderUpdateParts_updatesChangedFields_andResetsHeaderFields() {
        Map<String, AttributeValue> current = new LinkedHashMap<>();
        current.put("id", AttributeValue.builder().s("F1").build());
        current.put("file_name", AttributeValue.builder().s("old.csv").build());
        current.put("file_status", AttributeValue.builder().s("DONE").build());
        current.put("is_processed", AttributeValue.builder().n("1").build());
        current.put("process_stage", AttributeValue.builder().s(FileProcessStage.PROCESSING.toString()).build());
        current.put("updated_date", AttributeValue.builder().s("2025-01-01 00:00:00").build());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("file_name", "new.csv"); // changed
        payload.put("id", "F1");

        BuiltUpdate bu = util.buildHeaderUpdateParts(payload, current);

        
        assertEquals(5, bu.setParts.size());
        assertEquals(5, bu.updatedFields);

        String joined = String.join(";", bu.setParts);
        assertTrue(joined.contains("= :v0")); 
        assertTrue(joined.contains("#file_status = :fileStatusEmpty"));
        assertTrue(joined.contains("#is_processed = :zeroProcessed"));
        assertTrue(joined.contains("#process_stage = :processStageEmpty"));
        assertTrue(joined.contains("#updated_date = :now"));

        assertEquals("", bu.eav.get(":fileStatusEmpty").s());
        assertEquals("0", bu.eav.get(":zeroProcessed").n());
        assertEquals(FileProcessStage.UNPROCESSED.toString(), bu.eav.get(":processStageEmpty").s());
        assertNotNull(bu.eav.get(":now").s());
    }

     

    @Test
    void buildRollbackFromSnapshot_buildsOnlyForChangedFields() {
        Map<String, Object> before = new LinkedHashMap<>();
        before.put("id", "S1");
        before.put("a", "foo");
        before.put("b", 123);
        before.put("c", true);

        Map<String, AttributeValue> after = new LinkedHashMap<>();
        after.put("id", AttributeValue.builder().s("S1").build());
        after.put("a", AttributeValue.builder().s("foo").build());
        after.put("b", AttributeValue.builder().n("999").build());
        after.put("c", AttributeValue.builder().bool(true).build());

        BuiltUpdate rb = util.buildRollbackFromSnapshot(before, after);

        
        assertEquals(1, rb.setParts.size());
        assertEquals(1, rb.updatedFields);

        String part = rb.setParts.get(0);
        assertTrue(part.contains("="));
        boolean valueMatches = rb.eav.values().stream()
                .anyMatch(av -> "123".equals(av.n()));
        assertTrue(valueMatches);
    }
}
