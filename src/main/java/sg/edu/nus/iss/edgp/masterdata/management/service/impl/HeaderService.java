package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.IHeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVUploadHeader;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@Service
@RequiredArgsConstructor
public class HeaderService implements IHeaderService {
	
	@Value("${aws.dynamodb.table.master.data.header}")
	private String headerTableName;

	private final DynamoDbClient dynamoDbClient;

	@Override
	public void saveHeader(String tableName, MasterDataHeader header) {
		CSVUploadHeader csvUpHeader = new CSVUploadHeader(header);

		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(csvUpHeader.toItem()).build();

		dynamoDbClient.putItem(request);
	}

	@Override
	public Optional<MasterDataHeader> fetchOldestByStage(FileProcessStage stage) {
	    ScanRequest req = ScanRequest.builder()
	        .tableName(headerTableName.trim())
	        .filterExpression("#ps = :ps")
	        .expressionAttributeNames(Map.of("#ps", "process_stage"))
	        .expressionAttributeValues(Map.of(":ps", AttributeValue.builder().s(stage.name()).build()))
	        .projectionExpression("id, domain_name, organization_id, policy_id,uploaded_by, uploaded_date,total_rows_count")
	        .build();

	    Map<String, AttributeValue> resultItem = null;
	    String resultUploaded = null;

	    for (ScanResponse page : dynamoDbClient.scanPaginator(req)) {
	        for (Map<String, AttributeValue> item : page.items()) {
	            AttributeValue idAttr  = item.get("id");
	            AttributeValue upAttr  = item.get("uploaded_date");
	            if (idAttr == null || upAttr == null || idAttr.s() == null || upAttr.s() == null) continue;

	            String uploaded = upAttr.s();
	            if (resultUploaded == null || uploaded.compareTo(resultUploaded) < 0) {
	            	resultUploaded = uploaded;
	            	resultItem = item;
	            }
	        }
	    }

	    if (resultItem == null) return Optional.empty();

	    MasterDataHeader header = new MasterDataHeader();
	    header.setId(resultItem.get("id").s());
	    header.setDomainName(resultItem.get("domain_name").s());
	    header.setOrganizationId(resultItem.get("organization_id").s());
	    header.setPolicyId(resultItem.get("policy_id").s());
	    header.setUploadedBy(resultItem.get("uploaded_by").s());
	    header.setTotalRowsCount(Integer.parseInt(resultItem.get("total_rows_count").n()));

	    return Optional.of(header);
	}

 

	@Override
	public void updateFileStage(String fileId, FileProcessStage processStage) {
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    	String updatedDate = LocalDateTime.now(ZoneId.of("Asia/Singapore")).format(fmt);

		Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s(fileId).build());

		UpdateItemRequest req = UpdateItemRequest.builder()
				.tableName(headerTableName.trim()).key(key)
				.updateExpression("SET #ps = :ps, updated_date = :now")
				.expressionAttributeNames(Map.of("#ps", "process_stage"))
				.expressionAttributeValues(Map.of(":ps", AttributeValue.builder().s(processStage.name()).build(),
						 ":now",
						AttributeValue.builder().s(updatedDate).build()))
				.conditionExpression("attribute_exists(id)").returnValues(ReturnValue.UPDATED_NEW).build();

		dynamoDbClient.updateItem(req);
	}
	
	@Override
	public boolean filenameExists(String filename) {
	    String fn = filename == null ? null : filename.trim();
	    if (fn == null || fn.isEmpty()) {
	     
	        throw new IllegalArgumentException("filename must not be blank");
	    }

	    ScanRequest req = ScanRequest.builder()
	        .tableName(headerTableName.trim())
	        .filterExpression("#fn = :fn")
	        .expressionAttributeNames(Map.of("#fn", "file_name"))
	        .expressionAttributeValues(Map.of(":fn", AttributeValue.builder().s(fn).build()))
	        .select(Select.COUNT)
	        .build();

	    try {
	        for (ScanResponse page : dynamoDbClient.scanPaginator(req)) {
	            if (page.count() > 0) return true;
	        }
	        return false;
	    } catch (software.amazon.awssdk.services.dynamodb.model.DynamoDbException e) {
	        
	        System.out.println("filenameExists scan failed:"+ e.getMessage()+ e);
	        throw e;
	    }
	}


}
