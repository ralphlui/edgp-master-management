package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.service.IDynamicDetailService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@RequiredArgsConstructor
@Service
public class DynamicDetailService implements IDynamicDetailService {
	
	private final DynamoDbClient dynamoDbClient;
	
	@Override
	public void insertStagingMasterData(String tableName,Map<String, String> rawData) {
	    if (rawData == null || rawData.isEmpty()) {
	        throw new IllegalArgumentException("No data provided for insert.");
	    }
	    
	   
	    Map<String, AttributeValue> item = new HashMap<>();

	    if (!rawData.containsKey("id")) {
	        item.put("id", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
	    } else {
	        item.put("id", AttributeValue.builder().s(rawData.get("id")).build());
	    }

	    for (Map.Entry<String, String> entry : rawData.entrySet()) {
	        String column = entry.getKey().toLowerCase().trim();
	        String value = entry.getValue();
	        
	        if (column == null || column.trim().isEmpty()) continue;

	        AttributeValue attrVal = convertToAttributeValue(value);
	        item.put(column, attrVal);
	    }

	    PutItemRequest request = PutItemRequest.builder()
	            .tableName(tableName)
	            .item(item)
	            .build();

	    dynamoDbClient.putItem(request);
	}
	
	private AttributeValue convertToAttributeValue(String value) {
	    if (value == null || value.trim().isEmpty()) {
	        return AttributeValue.builder().nul(true).build();
	    }

	    String trimmed = value.trim();

	    try {
	        // Numeric detection
	        if (trimmed.matches("-?\\d+")) {
	            return AttributeValue.builder().n(trimmed).build(); // integer
	        } else if (trimmed.matches("-?\\d+\\.\\d+")) {
	            return AttributeValue.builder().n(trimmed).build(); // decimal
	        } else if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
	            return AttributeValue.builder().bool(Boolean.parseBoolean(trimmed)).build();
	        } else {
	            return AttributeValue.builder().s(trimmed).build(); // default to string
	        }
	    } catch (Exception e) {
	        return AttributeValue.builder().s(trimmed).build(); // fallback
	    }
	}
	
	@Override
	 public boolean tableExists(String tableName) {
	        try {
	            dynamoDbClient.describeTable(DescribeTableRequest.builder()
	                    .tableName(tableName).build());
	            return true;
	        } catch (ResourceNotFoundException e) {
	            return false;
	        }
	    }

	
	@Override
	public void createTable(String tableName) {
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        dynamoDbClient.createTable(request);
        // Wait until table is ACTIVE
        waitForTableToBecomeActive(tableName);
    }
	
	private void waitForTableToBecomeActive(String tableName) {
	    while (true) {
	        DescribeTableResponse response = dynamoDbClient.describeTable(DescribeTableRequest.builder()
	                .tableName(tableName)
	                .build());

	        String status = response.table().tableStatusAsString();
	        if ("ACTIVE".equalsIgnoreCase(status)) break;

	        try {
	            Thread.sleep(1000); // Wait 1 sec before checking again
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	            throw new RuntimeException("Interrupted while waiting for DynamoDB table to become active");
	        }
	    }
	}
	
	@Override
	public void insertValidatedMasterData(String tableName, Map<String, AttributeValue> rowData) {
	    if (rowData == null || rowData.isEmpty()) return;

	    PutItemRequest request = PutItemRequest.builder()
	        .tableName(tableName)
	        .item(rowData) 
	        .build();

	    dynamoDbClient.putItem(request);
	}

	
	@Override
	public List<Map<String, AttributeValue>> getUnprocessedRecordsByFileId(String tableName,
			String fileId,String policyId,String domainName) {
	    Map<String, AttributeValue> expressionValues = new HashMap<>();
	    expressionValues.put(":file_id", AttributeValue.builder().s(fileId).build());
	    expressionValues.put(":domain_name", AttributeValue.builder().s(domainName).build());
	    expressionValues.put(":policy_id", AttributeValue.builder().s(policyId).build());
	    expressionValues.put(":is_processed", AttributeValue.builder().n("0").build());

	    ScanRequest scanRequest = ScanRequest.builder()
	        .tableName(tableName)
	        .filterExpression("file_id = :file_id  AND domain_name = :domain_name AND policy_id = :policy_id AND is_processed = :is_processed")
	        .expressionAttributeValues(expressionValues)
	        .build();

	    List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

        if (results.isEmpty()) {
            throw new RuntimeException("Data not found in staging table." );
        }

	    return results;
	}

	
    @Override
    public void updateStagingProcessedStatus(String tableName, String id, String newStatus) {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String updatedDate = LocalDateTime.now(ZoneId.of("Asia/Singapore")).format(fmt);

        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s(id).build());

        UpdateItemRequest req = UpdateItemRequest.builder()
            .tableName(tableName)
            .key(key)
            .updateExpression("SET is_processed = :s, updated_date = :ud")
            .expressionAttributeValues(Map.of(
                ":s", AttributeValue.builder().s(newStatus).build(),
                ":ud", AttributeValue.builder().s(updatedDate).build()
            ))
            .conditionExpression("attribute_exists(id)")
            .build();

        dynamoDbClient.updateItem(req);
    }
    
    public Map<String, AttributeValue> getDomainNameByFileID(String tableName, String id) {
        try {
            GetItemRequest getItemRequest = GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("id", AttributeValue.builder().s(id).build()))
                    .build();

            GetItemResponse response = dynamoDbClient.getItem(getItemRequest);
            return response.item();
        } catch (DynamoDbException e) {
            System.err.println("Error retrieving item: " + e.getMessage());
            return null;
        }
    }


}
