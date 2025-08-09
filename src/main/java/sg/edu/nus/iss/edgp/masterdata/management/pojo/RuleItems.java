package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleItems {
	
	@JsonProperty("ruleId")
	private String ruleId;
	
	@JsonProperty("ruleName")
	private String ruleName;

	@JsonProperty("appliesToField")
	private String appliesToField;

	@JsonProperty("description")
	private String description;
	
	@JsonProperty("parameters")
	private String parameters;


}
