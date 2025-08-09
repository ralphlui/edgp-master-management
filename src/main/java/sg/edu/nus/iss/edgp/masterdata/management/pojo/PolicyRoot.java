package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class PolicyRoot {
	
	@JsonProperty("success")
	private Boolean success;
	
	@JsonProperty("totalRecord")
	private Integer totalRecord;
	
	@JsonProperty("data")
	private PolicyData data;

}
