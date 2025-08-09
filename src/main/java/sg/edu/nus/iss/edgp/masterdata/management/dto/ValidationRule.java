package sg.edu.nus.iss.edgp.masterdata.management.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValidationRule {
    private String rule_name;
    private String column_name;
    private Object value;
    private String rule_description;
}
