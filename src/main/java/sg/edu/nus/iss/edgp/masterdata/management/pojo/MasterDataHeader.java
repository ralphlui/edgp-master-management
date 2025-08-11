package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import lombok.Getter;
import lombok.Setter;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;

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
	private String updated_date="";
	private String uploadedBy="";
	private FileProcessStage processStage = FileProcessStage.UNPROCESSED;
	private boolean fileStatus=false;
	
}
