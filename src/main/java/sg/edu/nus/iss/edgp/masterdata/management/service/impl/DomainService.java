package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.service.IDomainService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DynamoConstants;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RequiredArgsConstructor
@Service
public class DomainService implements IDomainService {
	private static final Logger logger = LoggerFactory.getLogger(MasterdataService.class);

	private final DynamoDbClient dynamoDbClient;
	private final DynamicDetailService dynamoService;

	@Override
	public List<String> findDomains() {
		List<String> retList = new ArrayList<>();

		try {
			if (dynamoService.tableExists(DynamoConstants.DOMAIN_TABLE_NAME.trim())) {

				ScanRequest scanRequest = ScanRequest.builder().tableName(DynamoConstants.DOMAIN_TABLE_NAME.trim())
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

}
