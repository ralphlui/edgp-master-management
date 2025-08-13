package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;


public class CSVUploadHeader {

	DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private final String id;
    private final String fileName;
    private final String domainName;
    private final String organizationId;
    private final String policyId;
    private final String uploadedBy;
    private final String uploadedDate;
    private final String updatedDate;
    private final int totalRows;
    private final FileProcessStage processStage;
    private final String fileStatus;
    

    public CSVUploadHeader( MasterDataHeader header) {
        this.id = header.getId();
        this.fileName = header.getFileName();
        this.domainName = header.getDomainName();
        this.policyId = header.getPolicyId();
        this.organizationId = header.getOrganizationId();
        this.uploadedBy = header.getUploadedBy();
        this.uploadedDate = LocalDateTime.now(ZoneId.of("Asia/Singapore")).format(fmt);
        this.updatedDate = "";
        this.totalRows = header.getTotalRowsCount();
        this.processStage = header.getProcessStage();
        this.fileStatus = header.getFileStatus();
        
    }

    public Map<String, AttributeValue> toItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(id).build());
        item.put("file_name", AttributeValue.builder().s(fileName).build());
        item.put("domain_name", AttributeValue.builder().s(domainName).build());
        item.put("organization_id", AttributeValue.builder().s(organizationId).build());
        item.put("policy_id", AttributeValue.builder().s(policyId).build());
        item.put("uploaded_by", AttributeValue.builder().s(uploadedBy).build());
        item.put("uploaded_date", AttributeValue.builder().s(uploadedDate).build());
        item.put("updated_date", AttributeValue.builder().s(updatedDate).build());
        item.put("total_rows_count", AttributeValue.builder().n(String.valueOf(totalRows)).build());
        item.put("process_stage", AttributeValue.builder().s(String.valueOf(processStage)).build());
        item.put("file_status", AttributeValue.builder().s(fileStatus).build());
        
        return item;
    }
}
