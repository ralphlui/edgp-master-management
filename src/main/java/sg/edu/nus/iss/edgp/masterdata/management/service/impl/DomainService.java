package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.service.IDomainService;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RequiredArgsConstructor
@Service
public class DomainService implements IDomainService {
	
	@Value("${aws.dynamodb.table.domain}")
	private String domainTableName;
	
	private static final Logger logger = LoggerFactory.getLogger(MasterdataService.class);

	private final DynamoDbClient dynamoDbClient;
	private final DynamicDetailService dynamoService;

	@Override
	public List<String> findDomains() {
		List<String> retList = new ArrayList<>();

		try {
			if (dynamoService.tableExists(domainTableName.trim())) {

				ScanRequest scanRequest = ScanRequest.builder().tableName(domainTableName.trim())
						.attributesToGet("name").build();

				ScanResponse response = dynamoDbClient.scan(scanRequest);

				List<Map<String, AttributeValue>> items = response.items();

				logger.info("Total record in findDomains: {}", items.size());

				for (Map<String, AttributeValue> item : items) {
					if (item.containsKey("name") && item.get("name").s() != null) {
						retList.add(item.get("name").s());
					}
				}
			}

			return retList;

		} catch (Exception e) {
			logger.error("findDomains exception: {}", e.toString());
			throw new MasterdataServiceException("An error occurred while fetching domains", e);
		}
	}

	@Override
	 public boolean createDomain(String domainName) {
        try {
            // 1) Validate input
            if (domainName == null || domainName.trim().isEmpty()) {
                throw new IllegalArgumentException("domainName is required");
            }
            final String normalized = domainName.trim();

            // 2) Ensure table exists (use the TABLE NAME, not the domain name)
            if (!dynamoService.tableExists(domainTableName.trim())) {
                dynamoService.createTable(domainTableName.trim());  
                
            }
            

            // 3) Conditional put not to overwrite existing domain
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        	 
            String createdDate = LocalDateTime.now(ZoneId.of("Asia/Singapore")).format(fmt);

            PutItemRequest req = PutItemRequest.builder()
                    .tableName(domainTableName.trim())
                    .item(Map.of(
                            "name", AttributeValue.builder().s(normalized).build(),
                            "createdDate", AttributeValue.builder().s(createdDate).build()
                    ))
                    // Only write if "name" does NOT already exist
                    .conditionExpression("attribute_not_exists(#n)")
                    .expressionAttributeNames(Map.of("#n", "name"))
                    .build();

            dynamoDbClient.putItem(req);
            return true; // created successfully
        } catch (ConditionalCheckFailedException e) {
            // Item already exists
            return false;
        } catch (DynamoDbException | SdkClientException e) {
            // Log and rethrow or return false depending on your policy
            // logger.error("Failed to create domain", e);
            throw new RuntimeException("Failed to create domain: " + e.getMessage(), e);
            // return false;
        } catch (Exception ex) {
            // logger.error("Unexpected error in createDomain", ex);
            throw new RuntimeException("Unexpected error in createDomain: " + ex.getMessage(), ex);
            // return false;
        }
    }


}
