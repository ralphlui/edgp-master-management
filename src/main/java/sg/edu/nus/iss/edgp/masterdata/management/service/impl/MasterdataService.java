package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

@RequiredArgsConstructor
@Service
public class MasterdataService implements IMasterdataService {

	@Value("${aws.dynamodb.table.master.data.header}")
	private String headerTableName;

	@Value("${aws.dynamodb.table.master.data.staging}")
	private String stagingTableName;

	@Value("${aws.dynamodb.table.master.data.task.tracker}")
	private String mdataTaskTrackerTable;

	private static final Logger logger = LoggerFactory.getLogger(MasterdataService.class);

	private final DynamoDbClient dynamoDbClient;
	private final CSVParser csvParser;
	private final JWTService jwtService;
	private final DynamicDetailService dynamoService;
	private final HeaderService headerService;
	private final SQSPublishingService sqsPublishingService;
	private final StagingDataService stagingDataService;
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
			String orgId = jwtService.extractOrgIdFromToken(jwtToken);

			String fileName = file.getOriginalFilename();
			String headerId = UUID.randomUUID().toString();
			MasterDataHeader header = new MasterDataHeader();
			header.setFileName(fileName);
			header.setId(headerId);
			header.setDomainName(masterReq.getDomainName().trim());
			header.setOrganizationId(orgId.trim());
			header.setPolicyId(masterReq.getPolicyId().trim());
			header.setUploadedBy(uploadedBy);
			header.setTotalRowsCount(rows.size());
			header.setProcessStage(FileProcessStage.UNPROCESSED);

			if (!dynamoService.tableExists(headerTableName.trim())) {
				dynamoService.createTable(headerTableName.trim());
			}
			headerService.saveHeader(headerTableName.trim(), header);

			if (!dynamoService.tableExists(stagingTableName.trim())) {
				dynamoService.createTable(stagingTableName.trim());
			}

			InsertionSummary summary = stagingDataService.insertToStaging(stagingTableName.trim(), rows, orgId,
					masterReq.getPolicyId(), masterReq.getDomainName(), headerId, uploadedBy);

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

	private List<Map<String, Object>> mapItemsBK(List<Map<String, AttributeValue>> items) {
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

	private List<Map<String, Object>> mapItems(List<Map<String, AttributeValue>> items, String authorizationHeader) {

		List<Map<String, Object>> result = new ArrayList<>();

		for (Map<String, AttributeValue> item : items) {
			Map<String, Object> row = new HashMap<>();

			item.forEach((key, value) -> {
				if (value.s() != null) {
					row.put(key, value.s());
				} else if (value.n() != null) {
					row.put(key, new BigDecimal(value.n()));
				} else if (value.bool() != null) {
					row.put(key, value.bool());
				} else if (value.hasL()) {
					row.put(key, value.l());
				} else if (value.hasM()) {
					row.put(key, value.m());
				}
			});

			row.remove("is_processed");

			String fileStatus = asStringAndRemove(row, "file_status");
			if (fileStatus == "") {
				String processStage = asString(row.get("process_stage"));
				if (processStage.equals(FileProcessStage.UNPROCESSED.toString())) {
					fileStatus = FileProcessStage.UNPROCESSED.toString();
				} else if (processStage.equals(FileProcessStage.PROCESSING.toString())) {
					fileStatus = FileProcessStage.PROCESSING.toString();
				} else {
					fileStatus = "";
				}
				row.put("file_status", fileStatus);
			} else {
				row.put("file_status", fileStatus);
			}

			String policyId = asStringAndRemove(row, "policy_id");

			if (policyId != null && !policyId.isBlank()) {

				PolicyRoot policyRoot = jsonReader.getValidationRules(policyId, authorizationHeader);
				if (policyRoot != null) {
					Map<String, Object> policy = new HashMap<>();
					policy.put("id", policyId);
					policy.put("name", policyRoot.getData().getPolicyName());
					row.put("policy", policy);
				}
			}

			String orgId = asStringAndRemove(row, "organization_id");

			if (orgId != null && !orgId.isBlank()) {
				String orgName = jsonReader.getOrganizationName(orgId, authorizationHeader);

				Map<String, Object> organization = new HashMap<>();
				organization.put("id", orgId);
				organization.put("name", orgName);
				row.put("organization", organization);
			}

			result.add(row);
		}
		return result;
	}

	// --- helpers ---
	private static String asStringAndRemove(Map<String, Object> map, String key) {
		Object v = map.remove(key);
		return (v == null) ? null : String.valueOf(v);
	}

	private static String firstNonEmpty(String a, String b) {
		return (a != null && !a.isBlank()) ? a : b;
	}

	@Override
	public List<Map<String, Object>> getDataByPolicyAndDomainName(SearchRequest searchReq, String authorizationHeader) {

		if (!dynamoService.tableExists(stagingTableName.trim())) {
			logger.warn("Table {} does not exist.", stagingTableName.trim());
			return Collections.emptyList();
		}

		String jwtToken = authorizationHeader.substring(7);
		String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);

		Map<String, AttributeValue> expressionValues = new HashMap<>();
		expressionValues.put(":policyId", AttributeValue.builder().s(searchReq.getPolicyId().trim()).build());
		expressionValues.put(":domainName", AttributeValue.builder().s(searchReq.getDomainName().trim()).build());
		expressionValues.put(":uploaded_by", AttributeValue.builder().s(uploadedBy.trim()).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(stagingTableName.trim())
				.filterExpression("policy_id = :policyId AND domain_name = :domainName AND uploaded_by = :uploaded_by")
				.expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items(), authorizationHeader);
	}

	@Override
	public List<Map<String, Object>> getDataByPolicyId(SearchRequest searchReq, String authorizationHeader) {

		if (!dynamoService.tableExists(stagingTableName.trim())) {
			logger.warn("Table {} does not exist.", stagingTableName.trim());
			return Collections.emptyList();
		}

		String jwtToken = authorizationHeader.substring(7);
		String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);

		Map<String, AttributeValue> expressionValues = new HashMap<>();
		expressionValues.put(":policyId", AttributeValue.builder().s(searchReq.getPolicyId().trim()).build());
		expressionValues.put(":uploaded_by", AttributeValue.builder().s(uploadedBy.trim()).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(stagingTableName.trim())
				.filterExpression("policy_id = :policyId AND uploaded_by = :uploaded_by")
				.expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items(), authorizationHeader);
	}

	@Override
	public List<Map<String, Object>> getDataByDomainName(SearchRequest searchReq, String authorizationHeader) {

		if (!dynamoService.tableExists(stagingTableName.trim())) {
			logger.warn("Table {} does not exist.", stagingTableName.trim());
			return Collections.emptyList();
		}

		String jwtToken = authorizationHeader.substring(7);
		String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);

		Map<String, AttributeValue> expressionValues = new HashMap<>();
		expressionValues.put(":domainName", AttributeValue.builder().s(searchReq.getDomainName().trim()).build());
		expressionValues.put(":uploaded_by", AttributeValue.builder().s(uploadedBy.trim()).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(stagingTableName.trim())
				.filterExpression("domain_name = :domainName AND uploaded_by = :uploaded_by")
				.expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items(), authorizationHeader);
	}

	@Override
	public List<Map<String, Object>> getDataByFileId(SearchRequest searchReq, String authorizationHeader) {

		if (!dynamoService.tableExists(stagingTableName.trim())) {
			logger.warn("Table {} does not exist.", stagingTableName.trim());
			return Collections.emptyList();
		}

		String jwtToken = authorizationHeader.substring(7);
		String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);

		Map<String, AttributeValue> expressionValues = new HashMap<>();
		expressionValues.put(":file_id", AttributeValue.builder().s(searchReq.getFileId().trim()).build());
		expressionValues.put(":uploaded_by", AttributeValue.builder().s(uploadedBy.trim()).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(stagingTableName.trim())
				.filterExpression("file_id = :file_id AND uploaded_by = :uploaded_by")
				.expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items(), authorizationHeader);
	}

	@Override
	public List<Map<String, Object>> getAllData(String authorizationHeader) {

		String jwtToken = authorizationHeader.substring(7);
		String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);

		if (!dynamoService.tableExists(stagingTableName.trim())) {
			logger.warn("Table {} does not exist.", stagingTableName.trim());
			return Collections.emptyList();
		}

		Map<String, AttributeValue> expressionValues = Map.of(":uploaded_by",
				AttributeValue.builder().s(uploadedBy).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(stagingTableName.trim())
				.filterExpression("uploaded_by = :uploaded_by").expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);
		return mapItems(response.items(), authorizationHeader);
	}

	@Override
	public int processAndSendRawDataToSqs() {

		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
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
				int totalCount = file.get().getTotalRowsCount();
				String organizationId = file.get().getOrganizationId();

				if (fileId == null || fileId.isEmpty() || organizationId == null || organizationId.isEmpty()
						|| policyId == null || policyId.isEmpty() || domainName == null || domainName.isEmpty()
						|| uploadedBy == null || uploadedBy.isEmpty()) {
					throw new MasterdataServiceException(
							"Missing header info (fileId/policyId/domainName/Uploaded User).");
				}

				// 2) Unprocessed staging
				List<Map<String, AttributeValue>> records = dynamoService
						.getUnprocessedRecordsByFileId(stagingTableName.trim(), fileId, policyId, domainName);

				if (!dynamoService.tableExists(mdataTaskTrackerTable.trim())) {
					dynamoService.createTable(mdataTaskTrackerTable.trim());
				}

				int processedCount = 0;

				String createdDate = LocalDateTime.now(ZoneId.of("Asia/Singapore")).format(fmt);

				for (Map<String, AttributeValue> record : records) {
					try {
						Map<String, AttributeValue> item = new LinkedHashMap<>(record);

						String stgID = item.get("id").s();

						if (dynamoService.claimStagingRow(stagingTableName, stgID)) {
							logger.info("Update handled status: " + stgID);
							try {

								item.put("id",
										AttributeValue.builder().s(java.util.UUID.randomUUID().toString()).build());
								item.put("domain_name", AttributeValue.builder().s(domainName).build());

								// Remove staging-only metadata
								item.remove("is_processed");
								item.remove("uploaded_by");
								item.remove("uploaded_date");
								item.remove("organization_id");
								item.remove("file_id");
								item.remove("policy_id");
								item.remove("domain_name");
								item.remove("is_handled");
								item.remove("claimed_at");
								item.remove("processed_at");

								// (3) Send to SQS
								String sqsMessage = prepareJsonMessage(item, fileId, policyId, domainName, uploadedBy);
								if (!sqsMessage.isEmpty()) {
									sqsPublishingService.sendRecordToQueue(sqsMessage);

									item.put("created_date", AttributeValue.builder().s(createdDate).build());
									item.put("organization_id", AttributeValue.builder().s(organizationId).build());
									item.put("file_id", AttributeValue.builder().s(fileId).build());
									item.put("policy_id", AttributeValue.builder().s(policyId).build());
									item.put("domain_name", AttributeValue.builder().s(domainName).build());
									item.put("uploaded_by", AttributeValue.builder().s(uploadedBy).build());
									item.put("final_status", AttributeValue.builder().s("").build());
									item.put("rule_status", AttributeValue.builder().s("").build());
									item.put("failed_validations",
											AttributeValue.builder().l(Collections.emptyList()).build());

									// (4) Insert into Workflow Status table
									dynamoService.insertValidatedMasterData(mdataTaskTrackerTable.trim(), item);

									// (5) Mark file stage as processing
									headerService.updateFileStage(fileId, FileProcessStage.PROCESSING);

									// (6) Mark staging as processed
									dynamoService.markProcessed(stagingTableName, stgID);

									processedCount++;
								}
							} catch (Exception ex) {
								// sending failed — allow another pod to retry later
								dynamoService.revertClaim(stagingTableName, stgID);
								logger.error("Error processing id : {},{}", stgID, ex.getMessage());
							}
						}

					} catch (Exception ex) {
						logger.error("Error processing record : {}", ex.getMessage());

					}
				}
				if (processedCount > 0) {
					if (processedCount == totalCount)
						dynamoService.updateStagingProcessedStatus(headerTableName.trim(), fileId, "1");
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
				authHeader = "Bearer " + authHeader;
				PolicyRoot policyRoot = jsonReader.getValidationRules(policyId, authHeader);
				if (policyRoot != null) {

					// Validation Rules
					for (RuleItems ruleItem : policyRoot.getData().getRules()) {
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

	@Override
	public List<Map<String, Object>> getAllUploadFiles(String authorizationHeader) {

		String jwtToken = authorizationHeader.substring(7);
		String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);

		if (!dynamoService.tableExists(headerTableName.trim())) {
			logger.warn("Table {} does not exist.", headerTableName.trim());
			return Collections.emptyList();
		}

		Map<String, AttributeValue> expressionValues = Map.of(":uploaded_by",
				AttributeValue.builder().s(uploadedBy).build());

		ScanRequest scanRequest = ScanRequest.builder().tableName(headerTableName.trim())
				.filterExpression("uploaded_by = :uploaded_by").expressionAttributeValues(expressionValues).build();

		ScanResponse response = dynamoDbClient.scan(scanRequest);

		return mapItems(response.items(), authorizationHeader);
	}

	private static String asString(Object o) {
		return (o == null) ? null : String.valueOf(o);
	}

	private static boolean isBlank(String s) {
		return s == null || s.trim().isEmpty();
	}

	@Override
	public UploadResult updateDataToTable(Map<String, Object> data) {
		// 1) Require id
		if (!data.containsKey("id")) {
			 return new UploadResult("Missing 'id' field in request: ", 0, List.of());
		
		}
		String workflowId = String.valueOf(data.get("id"));
		 

        // 2. Lookup staging id from mapping table
        GetItemResponse mappingResp = dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName(mdataTaskTrackerTable.trim())
                .key(Map.of("id", AttributeValue.builder().s(workflowId).build()))
                .consistentRead(true)
                .build());

        if (!mappingResp.hasItem() || mappingResp.item().isEmpty()) {
            return new UploadResult("Data not found for id: " + workflowId, 0, List.of());
        }

        String stgId = mappingResp.item().get("stg_id").s();

        if(stgId.isEmpty() || stgId == null) {
        	 return new UploadResult("Data not found for id: " + workflowId, 0, List.of());
        
        }

		// 3) Load current item
		GetItemResponse get = dynamoDbClient.getItem(GetItemRequest.builder().tableName(stagingTableName)
				.key(Map.of("id", AttributeValue.builder().s(stgId).build())).consistentRead(true).build());

		if (!get.hasItem() || get.item().isEmpty()) {

			 return new UploadResult("Data not found for id: " + workflowId, 0, List.of());
		}

		Map<String, AttributeValue> current = get.item();

		Map<String, String> ean = new LinkedHashMap<>();
		Map<String, AttributeValue> eav = new LinkedHashMap<>();
		List<String> setParts = new ArrayList<>();
		int idx = 0;
		int updatedFields = 0;

		for (Map.Entry<String, Object> entry : data.entrySet()) {
			String field = entry.getKey();
			if ("id".equals(field))
				continue; // never update key
			if (!current.containsKey(field))
				continue; // skip new/unknown fields

			Object raw = entry.getValue();
			if (raw == null)
				continue; // skip nulls

			AttributeValue target = toAttr(raw);
			AttributeValue existing = current.get(field);

			if (equalsAttr(existing, target))
				continue; // skip if same

			String nameTok = "#n" + idx;
			String valTok = ":v" + idx;
			ean.put(nameTok, field);
			eav.put(valTok, target);
			setParts.add(nameTok + " = " + valTok);
			updatedFields++;
			idx++;
		}

		if (updatedFields == 0) {

			return new UploadResult("No changes applied (all fields skipped or identical).", 0,
					List.of(fromAttrMap(current)));
		}

		String updateExp = "SET " + String.join(", ", setParts);

		UpdateItemResponse resp = dynamoDbClient.updateItem(UpdateItemRequest.builder().tableName(stagingTableName)
				.key(Map.of("id", AttributeValue.builder().s(stgId).build())).conditionExpression("attribute_exists(#k)")
				.expressionAttributeNames(merge(ean, Map.of("#k", "id"))).expressionAttributeValues(eav)
				.updateExpression(updateExp).returnValues(ReturnValue.ALL_NEW).build());

		Map<String, Object> updated = fromAttrMap(resp.attributes());

		return new UploadResult("Updated " + updatedFields + " field(s).", updatedFields, List.of(updated));
	}

	private static Map<String, String> merge(Map<String, String> a, Map<String, String> b) {
		Map<String, String> m = new LinkedHashMap<>(a);
		m.putAll(b);
		return m;
	}

	private AttributeValue toAttr(Object v) {
		if (v instanceof Number)
			return AttributeValue.builder().n(String.valueOf(v)).build();
		if (v instanceof Boolean)
			return AttributeValue.builder().bool((Boolean) v).build();
		if (v instanceof Map)
			return AttributeValue.builder()
					.m(((Map<?, ?>) v).entrySet().stream()
							.collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> toAttr(e.getValue()))))
					.build();
		if (v instanceof List)
			return AttributeValue.builder().l(((List<?>) v).stream().map(this::toAttr).collect(Collectors.toList()))
					.build();

		return AttributeValue.builder().s(String.valueOf(v)).build();
	}

	private boolean equalsAttr(AttributeValue a, AttributeValue b) {
		if (a == null && b == null)
			return true;
		if (a == null || b == null)
			return false;

		if (a.s() != null || b.s() != null)
			return Objects.equals(a.s(), b.s());
		if (a.n() != null || b.n() != null)
			return Objects.equals(a.n(), b.n());
		if (a.bool() != null || b.bool() != null)
			return Objects.equals(a.bool(), b.bool());
		if (a.hasM() || b.hasM())
			return Objects.equals(a.m(), b.m());
		if (a.hasL() || b.hasL())
			return Objects.equals(a.l(), b.l());
		if (a.nul() != null || b.nul() != null)
			return Objects.equals(a.nul(), b.nul());
		if (a.hasSs() || b.hasSs())
			return Objects.equals(a.ss(), b.ss());
		if (a.hasNs() || b.hasNs())
			return Objects.equals(a.ns(), b.ns());
		if (a.hasBs() || b.hasBs())
			return Objects.equals(a.bs(), b.bs());
		return a.equals(b);
	}

	private Map<String, Object> fromAttrMap(Map<String, AttributeValue> attrs) {
		Map<String, Object> out = new LinkedHashMap<>();
		attrs.forEach((k, v) -> out.put(k, fromAttr(v)));
		return out;
	}

	private Object fromAttr(AttributeValue a) {
		if (a.s() != null)
			return a.s();
		if (a.n() != null)
			return new java.math.BigDecimal(a.n());
		if (a.bool() != null)
			return a.bool();
		if (a.hasM()) {
			return a.m().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> fromAttr(e.getValue())));
		}
		if (a.hasL()) {
			return a.l().stream().map(this::fromAttr).collect(Collectors.toList());
		}
		if (a.nul() != null && a.nul())
			return null;
		if (a.hasSs())
			return new java.util.HashSet<>(a.ss());
		if (a.hasNs())
			return new java.util.HashSet<>(a.ns());
		if (a.hasBs())
			return new java.util.HashSet<>(a.bs());
		return null;
	}

}
