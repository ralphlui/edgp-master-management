package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.service.IHeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVUploadHeader;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

@Service
@RequiredArgsConstructor
public class HeaderService implements IHeaderService{

    private final DynamoDbClient dynamoDbClient;

    @Override
    public void saveHeader(String tableName,String id, String filename, String uploadedBy) {
        CSVUploadHeader header = new CSVUploadHeader(id, filename, uploadedBy);

        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(header.toItem())
                .build();

        dynamoDbClient.putItem(request);
    }
    
    @Override
    public String getFileIdByFileName(String headerTableName, String fileName) {
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":fileName", AttributeValue.builder().s(fileName).build());

        ScanRequest scanRequest = ScanRequest.builder()
            .tableName(headerTableName)
            .filterExpression("fileName = :fileName")
            .expressionAttributeValues(expressionValues)
            .build();

        List<Map<String, AttributeValue>> results = dynamoDbClient.scan(scanRequest).items();

        if (results.isEmpty()) {
            throw new RuntimeException("fileName not found in header table: " + fileName);
        }

        return results.get(0).get("id").s();
    }

}
