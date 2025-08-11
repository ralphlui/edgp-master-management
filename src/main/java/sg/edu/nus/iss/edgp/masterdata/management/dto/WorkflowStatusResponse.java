package sg.edu.nus.iss.edgp.masterdata.management.dto;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowStatusResponse {
	
	@JsonProperty("file_id")
    private String fileId;
	
	@JsonProperty("status")
    private String status; // "true" or "false"
}
