package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.util.HashMap;
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
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

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
	        String column = entry.getKey();
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


}
