package sg.edu.nus.iss.edgp.masterdata.management.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SearchRequest {
	    
	    @NotBlank
	    private String domainName;

	    @Min(0)
	    private int page = 0;

	    @Min(1)
	    private int size = 50;

	    private String policyId;
	    private String organizationId;
}
