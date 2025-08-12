package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.*; 
import sg.edu.nus.iss.edgp.masterdata.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.masterdata.management.dto.InsertionSummary;
import sg.edu.nus.iss.edgp.masterdata.management.dto.Metadata;
import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationRule;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.service.IMasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVParser;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DynamoConstants;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;
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
	private final JSONWriterService stagingWriterService;
	private final PayloadBuilderService payloadBuilderService;
	private final JSONReader jsonReader;

	@Override
	public UploadResult uploadCsvDataToTable(MultipartFile file, UploadRequest masterReq, String authorizationHeader) {

		try {
			List<Map<String, String>> rows = csvParser.parseCsv(file);

			if (rows.isEmpty()) {
				return new UploadResult("CSV is empty.", 0, Collections.emptyList());
			}

			String jwtToken = authorizationHeader.substring(7);
			String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);

			String fileName = file.getOriginalFilename();
			String headerId = UUID.randomUUID().toString();
			MasterDataHeader header = new MasterDataHeader();
			header.setFileName(fileName);
			header.setId(headerId);
			header.setDomainName(masterReq.getDomainName().trim());
			header.setOrganizationId(masterReq.getOrganizationId().trim());
			header.setPolicyId(masterReq.getPolicyId().trim());
			header.setUploadedBy(uploadedBy);
			header.setTotalRowsCount(rows.size());
			header.setProcessStage(FileProcessStage.UNPROCESSED);

			String headerTableName = DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME;
			if (!dynamoService.tableExists(headerTableName)) {
				dynamoService.createTable(headerTableName);
			}
			headerService.saveHeader(headerTableName, header);

			String stagingTableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
			if (!dynamoService.tableExists(stagingTableName)) {
				dynamoService.createTable(stagingTableName);
			}

			InsertionSummary summary = stagingWriterService.writeToStaging(stagingTableName, rows,
					masterReq.getOrganizationId(), masterReq.getPolicyId(), masterReq.getDomainName(), headerId,
					uploadedBy);

			// Reply to FE with top 50 preview and the total count
			int total = summary.totalInserted();
			List<Map<String, Object>> top50 = summary.previewTop50();

			String message = "Uploaded " + total + " rows successfully.";
			return new UploadResult(message, total, top50);

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
	public int processAndSendRawDataToSqs() {
		String headerTable = DynamoConstants.MASTER_DATA_HEADER_TABLE_NAME.trim();
		String stagingTable = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME.trim();
		String workflowStatus = DynamoConstants.WORKFLOW_STATUS_TABLE_NAME.trim();

		try {

			// 1) Header lookup
			Optional<MasterDataHeader> file = headerService.fetchOldestByStage(FileProcessStage.UNPROCESSED);

			if (file.isEmpty()) {
				logger.info("File not found to process");

			} else {

				String fileId = file.get().getId();
				String policyId = file.get().getPolicyId();
				String domainName = file.get().getDomainName();
				String uploadedBy = file.get().getUploadedBy();
				int totalCount =file.get().getTotalRowsCount();
				String organizationId= file.get().getOrganizationId();

				if (fileId == null || fileId.isEmpty() || organizationId == null 
						|| organizationId.isEmpty() || policyId == null || policyId.isEmpty() || domainName == null
						|| domainName.isEmpty() || uploadedBy == null || uploadedBy.isEmpty()) {
					throw new MasterdataServiceException(
							"Missing header info (fileId/policyId/domainName/Uploaded User).");
				}

				// 2) Unprocessed staging
				List<Map<String, AttributeValue>> records = dynamoService.getUnprocessedRecordsByFileId(stagingTable,
						fileId, policyId, domainName);

				if (!dynamoService.tableExists(workflowStatus)) {
					dynamoService.createTable(workflowStatus);
				}

				int processedCount = 0;
				 
				String createdDateIso = java.time.LocalDateTime.now().toString();

				for (Map<String, AttributeValue> record : records) {
					try {

						Map<String, AttributeValue> item = new LinkedHashMap<>(record);

						String stgID = item.get("id").s();

						item.put("id", AttributeValue.builder().s(java.util.UUID.randomUUID().toString()).build());
						item.put("domain_name", AttributeValue.builder().s(domainName).build());

						// Remove staging-only metadata
						item.remove("is_processed");
						item.remove("uploaded_by");
						item.remove("uploaded_date");
						item.remove("organization_id");
						item.remove("file_id");
						item.remove("policy_id");
						item.remove("domain_name");
						
						// (3) Send to SQS
						String sqsMessage = prepareJsonMessage(item, fileId, policyId, domainName, uploadedBy);
						if (!sqsMessage.isEmpty()) {
							sqsPublishingService.sendRecordToQueue(sqsMessage);
							
							item.put("created_date", AttributeValue.builder().s(createdDateIso).build());
							item.put("organization_id", AttributeValue.builder().s(organizationId).build());
							item.put("file_id",AttributeValue.builder().s(fileId).build());
							item.put("policy_id",AttributeValue.builder().s(policyId).build());
							item.put("domain_name",AttributeValue.builder().s(domainName).build());
							item.put("uploaded_by",AttributeValue.builder().s(uploadedBy).build());
							item.put("final_status", AttributeValue.builder().s("").build());
							item.put("rule_status", AttributeValue.builder().l(Collections.emptyList()).build());

							// (4) Insert into Workflow Status table
							dynamoService.insertValidatedMasterData(workflowStatus, item);

							// (5) Mark file stage as processig
							headerService.updateFileStage(fileId, FileProcessStage.PROCESSING);

							// (6) Mark staging as processed
							dynamoService.updateStagingProcessedStatus(stagingTable, stgID, "1");

							processedCount++;
						}
					} catch (Exception ex) {
						logger.error("Error processing record : {}", ex.getMessage());

					}
				}
				if (processedCount > 0) {
					if (processedCount == totalCount)
						dynamoService.updateStagingProcessedStatus(headerTable, fileId, "1");
				}
				return processedCount;
			}

		} catch (Exception e) {
			logger.error("processAndSendToSqs exception: {}", e.toString());
			throw new MasterdataServiceException("Process and send data to SQS failed: " + e.getMessage());
		}
		return 0;

	}

	private static Object avToJava(AttributeValue v) {
		if (v == null)
			return null;
		if (v.s() != null)
			return v.s();
		if (v.n() != null)
			return new java.math.BigDecimal(v.n());
		if (v.bool() != null)
			return v.bool();
		if (Boolean.TRUE.equals(v.nul()))
			return null;

		if (v.ss() != null && !v.ss().isEmpty())
			return new java.util.ArrayList<>(v.ss());
		if (v.ns() != null && !v.ns().isEmpty())
			return v.ns().stream().map(java.math.BigDecimal::new).toList();
		if (v.bs() != null && !v.bs().isEmpty())
			return v.bs();

		if (v.l() != null && !v.l().isEmpty())
			return v.l().stream().map(MasterdataService::avToJava).toList();

		if (v.m() != null && !v.m().isEmpty()) {
			Map<String, Object> m = new LinkedHashMap<>();
			v.m().forEach((k, vv) -> m.put(k, avToJava(vv)));
			return m;
		}
		return null;
	}

	private static Map<String, Object> avMapToJava(Map<String, AttributeValue> item) {
		Map<String, Object> out = new LinkedHashMap<>(item.size());
		item.forEach((k, v) -> out.put(k, avToJava(v)));
		return out;
	}

	private String prepareJsonMessage(Map<String, AttributeValue> item, String fileId, String policyId,
			String domainName, String uploadedUser) throws com.fasterxml.jackson.core.JsonProcessingException {

		try {

			Metadata mData = new Metadata();
			mData.setDomainName(domainName.trim());
			mData.setFileId(fileId.trim());
			mData.setPolicyId(policyId.trim());

			Map<String, Object> payload = avMapToJava(item);

			List<ValidationRule> rules = new ArrayList<ValidationRule>();
			// find authHeader by uploadeduser
			String authHeader = jsonReader.getAccessToken(uploadedUser);
			

			if (authHeader != null && !authHeader.isEmpty()) {
				authHeader ="Bearer "+authHeader;
				PolicyRoot policyRoot = jsonReader.getValidationRules(policyId, authHeader);
				if (policyRoot != null) {
					

   				 //Validation Rules
   				 for(RuleItems ruleItem : policyRoot.getData().getRules()) {
   				 ValidationRule rule = new ValidationRule();
   				 rule.setRule_name(ruleItem.getRuleName());
   				 rule.setColumn_name(ruleItem.getAppliesToField());
   				 rule.setRule_description(ruleItem.getDescription());
   				 rule.setValue(ruleItem.getParameters());
   				 rules.add(rule);
   				 }

					return payloadBuilderService.build(mData, payload, rules);
				}
			}

		} catch (Exception e) {
			logger.error("prepareJsonMessageFromAv exception: {}", e.toString());
			throw new MasterdataServiceException("Prepare Json Message failed: " + e.getMessage());
		}
		return "";

	}

}
