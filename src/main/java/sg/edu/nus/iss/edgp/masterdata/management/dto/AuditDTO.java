package sg.edu.nus.iss.edgp.masterdata.management.dto;

import lombok.Getter;
import lombok.Setter;
import sg.edu.nus.iss.edgp.masterdata.management.enums.*;

@Getter
@Setter
public class AuditDTO {
	
	private int statusCode =0;
	private String userId="";
	private String username="";
	private String activityType="";
	private String activityDescription="";
	private String requestActionEndpoint ="";
	private AuditResponseStatus responseStatus ;
	private HTTPVerb requestType;
	private String remarks="";
	

}
