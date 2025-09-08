package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.InsertionSummary;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.DataIngestResult;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.IDataIngestService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.IngestRequest;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

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

	private final JWTService jwtService;
	private final DynamicDetailService dynamoService;
	private final HeaderService headerService;
	private final StagingDataService stagingDataService;


	public DataIngestResult processIngest(IngestRequest request, String authorizationHeader) {
	    
	    if (request == null || request.domainName() == null || request.domainName().trim().isEmpty()) {
	        throw new IllegalArgumentException("domain_name is mandatory.");
	    }
	    if (request.items() == null || request.items().isEmpty()) {
	        throw new IllegalArgumentException("items must not be empty.");
	    }

	    try {
	      
	        String jwtToken   = extractBearerToken(authorizationHeader);
	        String uploadedBy = jwtService.extractUserEmailFromToken(jwtToken);
	        String orgId      = jwtService.extractOrgIdFromToken(jwtToken);

	      
	        String headerId = UUID.randomUUID().toString();
	        MasterDataHeader header = new MasterDataHeader();
	        header.setId(headerId);
	        header.setFileName( "Data Ingest Workflow");
	        header.setDomainName(request.domainName().trim());
	        header.setOrganizationId(orgId == null ? "" : orgId.trim());
	        header.setPolicyId(request.policyId() == null ? "" : request.policyId().trim());
	        header.setUploadedBy(uploadedBy);
	        header.setTotalRowsCount(request.items().size());
	        header.setProcessStage(FileProcessStage.UNPROCESSED);
	        header.setUploadDate(Instant.now().toString());

	        if (!dynamoService.tableExists(headerTableName.trim())) {
	            dynamoService.createTable(headerTableName.trim());
	        }
	        headerService.saveHeader(headerTableName.trim(), header);

	        
	        List<LinkedHashMap<String, Object>> rows = request.items().stream()
	                .map(IngestRequest.Item::attributes)
	                .filter(Objects::nonNull)
	                .map(attrs -> attrs.entrySet().stream()
	                        .filter(e -> e.getKey() != null && e.getValue() != null)
	                        .collect(Collectors.toMap(
	                                e -> e.getKey().trim(),
	                                Map.Entry::getValue,
	                                (a, b) -> a,
	                                LinkedHashMap::new
	                        )))
	                .toList();

	        if (rows.isEmpty()) {
	            throw new IllegalArgumentException("All item attributes are null/empty; nothing to ingest.");
	        }

	        // 5) Ensure staging table exists, then insert
	        if (!dynamoService.tableExists(stagingTableName.trim())) {
	            dynamoService.createTable(stagingTableName.trim());
	        }
	        
	        InsertionSummary summary = stagingDataService.insertToStaging(
	                stagingTableName.trim(),
	                rows,
	                orgId,
	                request.policyId(),
	                request.domainName(),
	                headerId,
	                uploadedBy
	        );
 
	        int total = summary.totalInserted();
	       
	        DataIngestResult result = new DataIngestResult("Date create successfully.", total);
	          
	        return result;

	    } catch (ConditionalCheckFailedException ccfe) {
	        throw ccfe;
	    } catch (Exception ex) {
	        logger.error("processIngest failed", ex);
	        DataIngestResult err = new DataIngestResult("Data create failed",0);
	        
	        
	        return err;
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

}
