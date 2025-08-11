package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sg.edu.nus.iss.edgp.masterdata.management.dto.Metadata;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationRule;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class PayloadBuilderService {

    private final ObjectMapper mapper;

    public PayloadBuilderService() {
        this.mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public String build(Metadata meta, Map<String, Object> record, List<ValidationRule> rules) throws Exception {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode entry = root.putObject("data_entry");

        entry.put("data_type", meta.getDataType());
        entry.put("domain_name", meta.getDomainName());
        entry.put("file_id", meta.getFileId());
        entry.put("policy_id", meta.getPolicyId());

        // data from DB
        entry.set("data", mapper.valueToTree(record));

        // rules from external API
        ArrayNode arr = entry.putArray("validation_rules");
        for (ValidationRule r : rules) {
            ObjectNode rn = arr.addObject();
            rn.put("rule_name", r.getRule_name());
            rn.put("column_name", r.getColumn_name());
            if (r.getValue() == null) {
                rn.putNull("value");
            } else {
                rn.set("value", mapper.valueToTree(r.getValue()));
            }
            rn.put("rule_description", r.getRule_description());
        }
        String s =mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        System.out.println(s);

        return s;
    }
}