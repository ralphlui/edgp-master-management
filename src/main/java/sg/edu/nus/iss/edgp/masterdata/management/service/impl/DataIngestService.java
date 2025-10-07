package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.masterdata.management.dto.InsertionSummary;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.DataIngestResult;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.IDataIngestService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.GeneralUtility;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

@RequiredArgsConstructor
@Service
public class DataIngestService implements IDataIngestService {

	@Value("${aws.dynamodb.table.master.data.header}")
	private String headerTableName;

	@Value("${aws.dynamodb.table.master.data.staging}")
	private String stagingTableName;

	@Value("${aws.dynamodb.table.master.data.task.tracker}")
	private String mdataTaskTrackerTable;

	private static final Logger logger = LoggerFactory.getLogger(DataIngestService.class);

	private final DynamoDbClient dynamoDbClient;

	private final JWTService jwtService;
	private final DynamicDetailService dynamoService;
	private final HeaderService headerService;
	private final StagingDataService stagingDataService;
	private final MasterdataService masterdataService;

	public UploadResult processIngest( Map<String, Object> request, String authorizationHeader) {

		try {
	       
	        if (request == null || request.isEmpty()) {
	            return new UploadResult("Request body is required.", 0, List.of());
	        }
	        Object dataObj = request.get("data");
	        if (!(dataObj instanceof Map)) {
	            return new UploadResult("'data' must be an object.", 0, List.of());
	        }
	        Map<String, Object> data = (Map<String, Object>) dataObj;

	        //1) Required fields
	        final String domainName = trimToEmpty(data.get("domain_name"));
	        final String policyId   = trimToEmpty(data.get("policy_id"));
	        if (domainName.isEmpty()) throw new IllegalArgumentException("domain_name is mandatory.");
	        if (policyId.isEmpty())   throw new IllegalArgumentException("policy_id is mandatory.");

	        // 2) Identity: prefer payload uploaded_by; else JWT; else "system"
	        String uploadedBy = trimToEmpty(data.get("uploaded_by"));
	        String orgId = "";
	        final String jwtToken = extractBearerToken(authorizationHeader);
	        if (uploadedBy.isEmpty() && jwtToken != null) {
	            try {
	                uploadedBy = defaultIfBlank(jwtService.extractUserEmailFromToken(jwtToken), "system");
	            } catch (Exception ignored) {}
	        }
	        if (jwtToken != null) {
	            try { orgId = defaultIfBlank(jwtService.extractOrgIdFromToken(jwtToken), ""); } catch (Exception ignored) {}
	        }
	        if (uploadedBy.isEmpty()) uploadedBy = "system";

	        // ---- 3) Build header ----
	        String headerId = UUID.randomUUID().toString();
	        MasterDataHeader header = new MasterDataHeader();
	        header.setId(headerId);
	        header.setFileName("Data Ingest Workflow");
	        header.setDomainName(domainName);
	        header.setOrganizationId(orgId);
	        header.setPolicyId(policyId);
	        header.setUploadedBy(uploadedBy);
	        header.setProcessStage(FileProcessStage.UNPROCESSED);
	        header.setUploadDate(Instant.now().toString());

	        //4) Build single row: exclude meta keys
	        Set<String> reserved = Set.of("domain_name", "policy_id", "uploaded_by");
	        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
	        for (Map.Entry<String, Object> e : data.entrySet()) {
	            String key = e.getKey();
	            if (key == null) continue;
	            String k = key.trim();
	            if (k.isEmpty() || reserved.contains(k)) continue;
	            Object v = e.getValue();
	            if (v != null) row.put(k, v);
	        }
	        if (row.isEmpty()) {
	            return new UploadResult("No row fields provided in data.", 0, List.of());
	        }
	        header.setTotalRowsCount(1);

	        // 5) Ensure tables exist, then save header
	        if (!dynamoService.tableExists(headerTableName.trim())) {
	            dynamoService.createTable(headerTableName.trim());
	        }
	        headerService.saveHeader(headerTableName.trim(), header);

	        //6) Ensure staging table exists, then insert
	        if (!dynamoService.tableExists(stagingTableName.trim())) {
	            dynamoService.createTable(stagingTableName.trim());
	        }
	        InsertionSummary summary = stagingDataService.insertToStaging(
	                stagingTableName.trim(),
	                List.of(row),
	                orgId,
	                policyId,
	                domainName,
	                headerId,
	                uploadedBy
	        );

	        return new UploadResult("Data created successfully.", summary.totalInserted(), summary.previewTop50());

	    } catch (ConditionalCheckFailedException ccfe) {
	        throw ccfe;
	    } catch (IllegalArgumentException iae) {
	        return new UploadResult(iae.getMessage(), 0, List.of());
	    } catch (Exception ex) {
	        logger.error("processIngest failed", ex);
	        return new UploadResult("Data create failed.", 0, List.of());
	    }
	}

	

	private String extractBearerToken(String authorizationHeader) {
		if (authorizationHeader == null || authorizationHeader.isBlank()) {
			throw new IllegalArgumentException("Missing Authorization header.");
		}
		String h = authorizationHeader.trim();
		if (h.regionMatches(true, 0, "Bearer ", 0, 7)) {
			return h.substring(7).trim();
		}
		return h;
	}

	public UploadResult updateDataToTable(Map<String, Object> data, String authorizationHeader) {
		try {

			String jwtToken = extractBearerToken(authorizationHeader);
			String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);
			String orgId = jwtService.extractOrgIdFromToken(jwtToken);
			data.put("organization_id", orgId);
			data.put("uploaded_by", uploadedBy);
			
		
			return masterdataService.updateDataToTable(data);

		} catch (ConditionalCheckFailedException ccfe) {
			throw ccfe;
		} catch (Exception ex) {
			logger.error("process re-ngest failed", ex);
			
		}
		return null;
	}
	
	private static String trimToEmpty(Object o) {
	    return o == null ? "" : o.toString().trim();
	}

	private static String defaultIfBlank(String s, String d) {
	    return (s == null || s.isBlank()) ? d : s;
	}

}
