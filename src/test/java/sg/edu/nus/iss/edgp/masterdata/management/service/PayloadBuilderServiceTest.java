package sg.edu.nus.iss.edgp.masterdata.management.service;


import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sg.edu.nus.iss.edgp.masterdata.management.dto.Metadata;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationRule;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.PayloadBuilderService;

public class PayloadBuilderServiceTest {

    private PayloadBuilderService svc;
    private final ObjectMapper om = new ObjectMapper();

    @BeforeEach
    void setUp() {
        svc = new PayloadBuilderService();
    }

    @Test
    void build_withRulesAndData_generatesExpectedJson() throws Exception {
        
        Metadata meta = new Metadata();
        meta.setDomainName("customer");
        meta.setFileId("F-123");
        meta.setPolicyId("POL-9");
        meta.setDataType(null);

        
        Map<String, Object> addr = Map.of("city", "SG", "zip", "12345");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", "S-1");
        record.put("name", "Alice");
        record.put("age", 30);
        record.put("address", addr);

      
        ValidationRule r1 = new ValidationRule();
        r1.setRule_name("between");
        r1.setColumn_name("age");
        r1.setRule_description("Age must be between 18 and 65");
        r1.setValue(Map.of("min", 18, "max", 65));

        ValidationRule r2 = new ValidationRule();
        r2.setRule_name("required");
        r2.setColumn_name("name");
        r2.setRule_description("Name is required");
        r2.setValue(null);

        String json = svc.build(meta, record, List.of(r1, r2));
        JsonNode root = om.readTree(json);

         
        JsonNode entry = root.get("data_entry");
        assertNotNull(entry, "data_entry should exist");

         
        assertTrue(entry.has("data_type"));
        assertTrue(entry.get("data_type").isNull(), "data_type should serialize as null");
        assertEquals("customer", entry.get("domain_name").asText());
        assertEquals("F-123", entry.get("file_id").asText());
        assertEquals("POL-9", entry.get("policy_id").asText());
 
        JsonNode data = entry.get("data");
        assertEquals("S-1", data.get("id").asText());
        assertEquals("Alice", data.get("name").asText());
        assertEquals(30, data.get("age").asInt());
        assertEquals("SG", data.get("address").get("city").asText());
        assertEquals("12345", data.get("address").get("zip").asText());

         
        JsonNode rules = entry.get("validation_rules");
        assertTrue(rules.isArray());
        assertEquals(2, rules.size());

        
        JsonNode jR1 = rules.get(0);
        assertEquals("between", jR1.get("rule_name").asText());
        assertEquals("age", jR1.get("column_name").asText());
        assertEquals("Age must be between 18 and 65", jR1.get("rule_description").asText());
        assertEquals(18, jR1.get("value").get("min").asInt());
        assertEquals(65, jR1.get("value").get("max").asInt());

       
        JsonNode jR2 = rules.get(1);
        assertEquals("required", jR2.get("rule_name").asText());
        assertEquals("name", jR2.get("column_name").asText());
        assertEquals("Name is required", jR2.get("rule_description").asText());
        assertTrue(jR2.get("value").isNull(), "value should be null for second rule");
    }

    @Test
    void build_emptyRules_emitsEmptyArray() throws Exception {
        Metadata meta = new Metadata();
        meta.setDomainName("orders");
        meta.setFileId("F-9");
        meta.setPolicyId("POL-Z");
        meta.setDataType("staging");

        Map<String, Object> record = Map.of("id", "S-9", "amount", 12.5);

        String json = svc.build(meta, record, Collections.emptyList());
        JsonNode entry = om.readTree(json).get("data_entry");

        assertEquals("staging", entry.get("data_type").asText());
        assertEquals("orders", entry.get("domain_name").asText());
        assertEquals("F-9", entry.get("file_id").asText());
        assertEquals("POL-Z", entry.get("policy_id").asText());

        JsonNode rules = entry.get("validation_rules");
        assertTrue(rules.isArray());
        assertEquals(0, rules.size());

        JsonNode data = entry.get("data");
        assertEquals("S-9", data.get("id").asText());
        assertEquals(12.5, data.get("amount").asDouble(), 1e-9);
    }

    @Test
    void build_handlesComplexValueTypes() throws Exception {
        Metadata meta = new Metadata();
        meta.setDomainName("product");
        meta.setFileId("F-77");
        meta.setPolicyId("POL-P");
        meta.setDataType("final");

        Map<String, Object> record = Map.of("sku", "A-1");

        
        ValidationRule rl = new ValidationRule();
        rl.setRule_name("in_set");
        rl.setColumn_name("category");
        rl.setRule_description("Must be allowed category");
        rl.setValue(Map.of("allowed", List.of("A", "B", "C")));

        String json = svc.build(meta, record, List.of(rl));
        JsonNode root = om.readTree(json);
        JsonNode valueNode = root.get("data_entry").get("validation_rules").get(0).get("value");
        assertNotNull(valueNode);
        assertTrue(valueNode.get("allowed").isArray());
        assertEquals(List.of("A","B","C"),
                asListOfText(valueNode.get("allowed")));
    }

    private static List<String> asListOfText(JsonNode arr) {
        List<String> out = new ArrayList<>(arr.size());
        arr.forEach(n -> out.add(n.asText()));
        return out;
    }
}

