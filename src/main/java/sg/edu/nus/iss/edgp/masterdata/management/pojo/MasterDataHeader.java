package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MasterDataHeader {
	
	private String id="";
	private String fileName="";
	private String policyId="";
	private String organizationId="";
	private String domainName="";
	private int totalRowsCount=0;
	private String uploadDate="";
	private String uploadedBy="";
	

}
