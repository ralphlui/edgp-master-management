package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.service.IHeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVUploadHeader;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

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
}
