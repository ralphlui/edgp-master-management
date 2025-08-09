package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class PolicyData {
	
	@JsonProperty("policyId")
	private String policyId;
	
	@JsonProperty("policyName")
	private String policyName;
	
	@JsonProperty("domainName")
	private String domainName;
	
	@JsonProperty("rules")
	private List<RuleItems> rules;
	
	@JsonProperty("createdBy")
	private String createdBy;
	
	@JsonProperty("lastUpdatedBy")
	private String lastUpdatedBy;
	
	@JsonProperty("organizationId")
	private String organizationId;
	
	@JsonProperty("published")
	private Boolean published;
	

}
