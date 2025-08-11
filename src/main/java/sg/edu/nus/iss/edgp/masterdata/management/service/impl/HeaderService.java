package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.IHeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVUploadHeader;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DynamoConstants;
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
	        .tableName(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim())
	        .filterExpression("#ps = :ps")
	        .expressionAttributeNames(Map.of("#ps", "process_stage"))
	        .expressionAttributeValues(Map.of(":ps", AttributeValue.builder().s(stage.name()).build()))
	        .projectionExpression("id, domain_name, organization_id, policy_id, uploaded_date")
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
	    

	    return Optional.of(header);
	}

 

	@Override
	public void updateFileStage(String fileId, FileProcessStage processStage) {

		Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s(fileId).build());

		UpdateItemRequest req = UpdateItemRequest.builder()
				.tableName(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim()).key(key)
				.updateExpression("SET #ps = :ps, updated_date = :now")
				.expressionAttributeNames(Map.of("#ps", "process_stage"))
				.expressionAttributeValues(Map.of(":ps", AttributeValue.builder().s(processStage.name()).build(),
						 ":now",
						AttributeValue.builder().s(java.time.Instant.now().toString()).build()))
				.conditionExpression("attribute_exists(id)").returnValues(ReturnValue.UPDATED_NEW).build();

		dynamoDbClient.updateItem(req);
	}
	
	@Override
	public boolean filenameExists(String filename) {
	    ScanRequest req = ScanRequest.builder()
	        .tableName(DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim())
	        .filterExpression("#fn = :fn")
	        .expressionAttributeNames(Map.of("#fn", "file_name"))
	        .expressionAttributeValues(Map.of(":fn", AttributeValue.builder().s(filename).build()))
	        .select(Select.COUNT)
	        .build();

	    for (ScanResponse page : dynamoDbClient.scanPaginator(req)) {
	        if (page.count() > 0) return true;
	    }
	    return false;
	}


}
