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

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.service.IMasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVParser;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DynamoConstants;
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

			String headerTableName = DynamoConstants.UPLOAD_HEADER_TABLE_NAME;
			if (!dynamoService.tableExists(headerTableName)) {
				dynamoService.createTable(headerTableName);
			}
			headerService.saveHeader(headerTableName, headerId, fileName, uploadedBy);

			String stagingTableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
			if (!dynamoService.tableExists(stagingTableName)) {
				dynamoService.createTable(stagingTableName);
			}

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

			for (Map<String, String> row : rows) {
				String uploadedDate = LocalDateTime.now().format(formatter);
				row.put("id", UUID.randomUUID().toString());
				row.put("organization_id", masterReq.getOrganizationId());
				row.put("policy_id", masterReq.getPolicyId());
				row.put("fileId", headerId);
				row.put("uploadedBy", uploadedBy);
				row.put("uploadedDate", uploadedDate);
				row.put("isprocessed", "0");

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
		String headerTable = DynamoConstants.UPLOAD_HEADER_TABLE_NAME;
		String stagingTable = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
		String masterDataTable = DynamoConstants.MASTER_DATA_TABLE_NAME;
		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

			String jwtToken = authorizationHeader.substring(7);
			String uploadedBy = jwtService.extractSubject(jwtToken);
			
			// Step 1: Get fileId from header
			String fileId = headerService.getFileIdByFileName(headerTable, fileName);

			// Step 2: Get unprocessed rows from staging table
			List<Map<String, AttributeValue>> records = dynamoService.getUnprocessedRecordsByFileId(stagingTable,
					fileId, uploadedBy);
			
			int processedCount = 0;

			for (Map<String, AttributeValue> record : records) {
				try {
					Map<String, String> validatedRow = record.entrySet().stream().collect(
							Collectors.toMap(Map.Entry::getKey, e -> e.getValue().s() != null ? e.getValue().s() : ""));

					String createdDate = LocalDateTime.now().format(formatter);
					validatedRow.put("createdDate", createdDate);
					validatedRow.put("status", "");
					validatedRow.put("ruleStatus", "");
					validatedRow.put("aiStatus", "");
					validatedRow.remove("isprocessed");
					validatedRow.remove("uploadedBy");
					validatedRow.remove("uploadedDate");

					// 1. Insert into validated table
					if (!dynamoService.tableExists(masterDataTable)) {
						dynamoService.createTable(masterDataTable);
					}
					dynamoService.insertValidatedMasterData(masterDataTable, validatedRow);

					// 2. Send to SQS
					validatedRow.remove("createdDate");
					sqsPublishingService.sendRecordToQueue(validatedRow);

					// 3. Update isprocessed
					String rowId = validatedRow.get("id");
					dynamoService.updateStagingProcessedStatus(stagingTable, rowId, "1");

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

}
