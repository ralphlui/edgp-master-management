package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.nimbusds.jose.shaded.gson.stream.JsonReader;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyData;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.RuleItems;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.masterdata.management.dto.Metadata;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationRule;
import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.service.IMasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVParser;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DynamoConstants;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JsonDataMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RequiredArgsConstructor
@Service
public class MasterdataService implements IMasterdataService {

	private static final Logger logger = LoggerFactory.getLogger(MasterdataService.class);

	private final DynamoDbClient dynamoDbClient;
	private final CSVParser csvParser;
	private final JWTService jwtService;
	private final DynamicDetailService dynamoService;
	private final HeaderService headerService;
	private final SQSPublishingService sqsPublishingService;
	private final JSONReader jsonReader;
	private final PayloadBuilderService builderService;

	@Override
	public UploadResult uploadCsvDataToTable(MultipartFile file, UploadRequest masterReq, String authorizationHeader) {

		List<Map<String, Object>> allInsertedRows = new ArrayList<>();

		try {
			List<Map<String, String>> rows = csvParser.parseCsv(file);
			if (rows.isEmpty()) {
				return new UploadResult("CSV is empty.", 0, Collections.emptyList());
			}

			String jwtToken = authorizationHeader.substring(7);
			String uploadedBy = jwtService.extractSubject(jwtToken);
			
			String fileName = file.getOriginalFilename();
			String headerId = UUID.randomUUID().toString();
			MasterDataHeader header = new MasterDataHeader();
			header.setFileName(fileName);
			header.setDomainName(masterReq.getDomainName().trim());
			header.setOrganizationId(masterReq.getOrganizationId().trim());
			header.setPolicyId(masterReq.getPolicyId().trim());
			header.setUploadedBy(uploadedBy);
			header.setTotalRowsCount(rows.size());
			

			String headerTableName = DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME;
			if (!dynamoService.tableExists(headerTableName)) {
				dynamoService.createTable(headerTableName);
			}
			headerService.saveHeader(headerTableName, header);

			String stagingTableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
			if (!dynamoService.tableExists(stagingTableName)) {
				dynamoService.createTable(stagingTableName);
			}

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

			for (Map<String, String> row : rows) {
				String uploadedDate = LocalDateTime.now().format(formatter);
				row.put("id", UUID.randomUUID().toString());
				row.put("organization_id", masterReq.getOrganizationId().trim());
				row.put("policy_id", masterReq.getPolicyId().trim());
				row.put("domain_name", masterReq.getDomainName().trim());
				row.put("file_id", headerId);
				row.put("uploaded_by", uploadedBy);
				row.put("uploaded_date", uploadedDate);
				row.put("is_processed", "0");

				dynamoService.insertStagingMasterData(stagingTableName, row);
				allInsertedRows.add(new HashMap<>(row));
			}

			// Slice to top 50 for display
			List<Map<String, Object>> top50 = allInsertedRows.stream().limit(50).collect(Collectors.toList());

			String message = "Inserted " + allInsertedRows.size() + " rows successfully.";
			return new UploadResult(message, allInsertedRows.size(), top50);

		} catch (Exception e) {
			logger.error("uploadCsvDataToTable exception: {}", e.toString());
			throw new MasterdataServiceException("Upload failed: " + e.getMessage());
		}
	}

	private List<Map<String, Object>> mapItems(List<Map<String, AttributeValue>> items) {
		List<Map<String, Object>> result = new ArrayList<>();
		for (Map<String, AttributeValue> item : items) {
			Map<String, Object> row = new HashMap<>();
			item.forEach((key, value) -> {
				if (value.s() != null)
					row.put(key, value.s());
				else if (value.n() != null)
					row.put(key, new BigDecimal(value.n()));
				else if (value.bool() != null)
					row.put(key, value.bool());
				else if (value.hasL())
					row.put(key, value.l());
				else if (value.hasM())
					row.put(key, value.m());
			});
			result.add(row);
		}
		return result;
	}

	@Override
	public List<Map<String, Object>> getDataByPolicyAndOrgId(SearchRequest searchReq) {

		String tableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
		if (!dynamoService.tableExists(tableName)) {
			logger.warn("Table {} does not exist.", tableName);
			return Collections.emptyList();
		}

		Map<String, AttributeValue> expressionValues = new HashMap<>();
		expressionValues.put(":orgId", AttributeValue.builder().s(searchReq.getOrganizationId()).build());
		expressionValues.put(":policyId", AttributeValue.builder().s(searchReq.getPolicyId()).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(tableName)
				.filterExpression("organization_id = :orgId AND policy_id = :policyId")
				.expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items());
	}

	@Override
	public List<Map<String, Object>> getDataByPolicyId(SearchRequest searchReq) {
		String tableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
		if (!dynamoService.tableExists(tableName)) {
			logger.warn("Table {} does not exist.", tableName);
			return Collections.emptyList();
		}

		Map<String, AttributeValue> expressionValues = Map.of(":policyId",
				AttributeValue.builder().s(searchReq.getPolicyId()).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).filterExpression("policy_id = :policyId")
				.expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items());
	}

	@Override
	public List<Map<String, Object>> getDataByOrgId(SearchRequest searchReq) {
		String tableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
		if (!dynamoService.tableExists(tableName)) {
			logger.warn("Table {} does not exist.", tableName);
			return Collections.emptyList();
		}

		Map<String, AttributeValue> expressionValues = Map.of(":orgId",
				AttributeValue.builder().s(searchReq.getOrganizationId()).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(tableName)
				.filterExpression("organization_id = :orgId").expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items());
	}

	@Override
	public List<Map<String, Object>> getAllData(SearchRequest searchReq) {
		String tableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
		if (!dynamoService.tableExists(tableName)) {
			logger.warn("Table {} does not exist.", tableName);
			return Collections.emptyList();
		}

		ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items());
	}

     @Override
	public int processAndSendRawDataToSqs(String fileName, String authorizationHeader) {
		String headerTable = DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim();
		String stagingTable = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME.trim();
		String masterDataTaskTable = DynamoConstants.MASTER_DATA_TASK_TRACKER_TABLE_NAME.trim();
		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

			String jwtToken = authorizationHeader.substring(7);
			String uploadedBy = jwtService.extractSubject(jwtToken);
		
			
			// Step 1: Get fileId from header
			List<Map<String, AttributeValue>> files = headerService.getFileByFileName(headerTable, fileName);
			String fileId = files.get(0).get("id").s();

			// Step 2: Get unprocessed rows from staging table
			List<Map<String, AttributeValue>> records = dynamoService.getUnprocessedRecordsByFileId(stagingTable,
					fileId, uploadedBy);
			
			int processedCount = 0;

			for (Map<String, AttributeValue> record : records) {
				try {
					Map<String, String> validatedRow = record.entrySet().stream().collect(
							Collectors.toMap(Map.Entry::getKey, e -> e.getValue().s() != null ? e.getValue().s() : ""));

					String createdDate = LocalDateTime.now().format(formatter);
					String stgID = validatedRow.get("id");
					validatedRow.remove("id");
					validatedRow.put("id", UUID.randomUUID().toString());
					validatedRow.put("created_date", createdDate);
					validatedRow.put("final_status", "");
					validatedRow.put("rule_status", "");
					validatedRow.put("message", "");
					validatedRow.remove("is_processed");
					validatedRow.remove("uploaded_by");
					validatedRow.remove("uploaded_date");
					validatedRow.remove("created_date");
					validatedRow.remove("organization_id");
					validatedRow.remove("file_id");
					validatedRow.remove("policy_id");
					validatedRow.remove("totalRowsCount");

					// 1. Insert into validated table
					if (!dynamoService.tableExists(masterDataTaskTable)) {
						dynamoService.createTable(masterDataTaskTable);
					}
					dynamoService.insertValidatedMasterData(masterDataTaskTable, validatedRow);

					// 2. Send to SQS
					//prepare JSON format
					String sqsMessage= prepareJsonMessage(validatedRow,fileId,authorizationHeader);
					sqsPublishingService.sendRecordToQueue(sqsMessage);

					// 3. Update isprocessed
					dynamoService.updateStagingProcessedStatus(stagingTable, stgID, "1");

					processedCount++;
				} catch (Exception e) {
					logger.error("Error processing record from file '{}': {}", fileName, e.getMessage());
				}
			}

			return processedCount;

		} catch (Exception e) {
			logger.error("processAndSendToSqs exception: {}", e.toString());
			throw new MasterdataServiceException("Process and send data to SQS failed: " + e.getMessage());
		}
	}
     
     private String prepareJsonMessage(Map<String, String> validatedRow,String fileID,String authorizationHeader) {
    	 String jsonMessage="";
	     try {
    	 PolicyRoot policyRoot= jsonReader.getValidationRules(validatedRow.get("policy_id"), authorizationHeader);
    	 
    	 if(policyRoot !=null) {
    		 if(policyRoot.getSuccess() && policyRoot.getTotalRecord()>0) {
    			 List<ValidationRule> rules = new ArrayList<ValidationRule>();
    				 Metadata metadata = new Metadata();
    		    	 metadata.setDomainName(policyRoot.getData().getDomainName());
    		    	 metadata.setFileId(fileID);
    				 metadata.setPolicyId(policyRoot.getData().getPolicyId());
    				
    				 //Validation Rules
    				 for(RuleItems ruleItem : policyRoot.getData().getRules()) {
    				 ValidationRule rule = new ValidationRule();
    				 rule.setRule_name(ruleItem.getRuleName());
    				 rule.setColumn_name(ruleItem.getAppliesToField());
    				 rule.setRule_description(ruleItem.getDescription());
    				 rule.setValue(ruleItem.getParameters());
    				 rules.add(rule);
    				 }
    				 
    				 jsonMessage= builderService.build(metadata,validatedRow, rules);
    		 }
    	 
    	 }
    	 
	     } catch (Exception e) {
				logger.error("prepareJsonMessage exception: {}", e.toString());
				throw new MasterdataServiceException("PrepareJsonMessage failed: " + e.getMessage());
			} 
	     return jsonMessage;
     }

}
