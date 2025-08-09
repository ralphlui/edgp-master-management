package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;


public class CSVUploadHeader {

	private final String id;
    private final String fileName;
    private final String domainName;
    private final String organizationId;
    private final String policyId;
    private final String uploadedBy;
    private final String uploadDate;
    private final int totalRows;

    public CSVUploadHeader( MasterDataHeader header) {
        this.id = header.getId();
        this.fileName = header.getFileName();
        this.domainName = header.getDomainName();
        this.policyId = header.getPolicyId();
        this.organizationId = header.getOrganizationId();
        this.uploadedBy = header.getUploadedBy();
        this.uploadDate = LocalDateTime.now().toString();
        this.totalRows = header.getTotalRowsCount();
    }

    public Map<String, AttributeValue> toItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(id).build());
        item.put("file_name", AttributeValue.builder().s(fileName).build());
        item.put("domain_name", AttributeValue.builder().s(domainName).build());
        item.put("organization_id", AttributeValue.builder().s(organizationId).build());
        item.put("policy_id", AttributeValue.builder().s(policyId).build());
        item.put("uploaded_by", AttributeValue.builder().s(uploadedBy).build());
        item.put("upload_date", AttributeValue.builder().s(uploadDate).build());
        item.put("total_rows_count", AttributeValue.builder().n(String.valueOf(totalRows)).build());
        return item;
    }
}
