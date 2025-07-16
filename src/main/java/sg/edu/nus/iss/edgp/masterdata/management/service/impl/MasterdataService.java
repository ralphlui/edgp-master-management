package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.aws.service.DynamoConstants;
import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.TemplateFileFormat;
import sg.edu.nus.iss.edgp.masterdata.management.repository.MetadataRepository;
import sg.edu.nus.iss.edgp.masterdata.management.service.IMasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVParser;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RequiredArgsConstructor
@Service
public class MasterdataService implements IMasterdataService {
	
	private static final Logger logger = LoggerFactory.getLogger(MasterdataService.class);

	
	private final MetadataRepository metadataRepository;
	private final CSVParser csvParser;
	private final JWTService jwtService;
	private final DynamicDetailService dynamoService;
	private final HeaderService headerService;
	
	
	
    @Override
    public UploadResult uploadCsvDataToTable(
            MultipartFile file, UploadRequest masterReq, String authorizationHeader) {

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

            String masterTableName = DynamoConstants.MASTER_DATA_STAGING_TABLE_NAME;
            if (!dynamoService.tableExists(masterTableName)) {
                dynamoService.createTable(masterTableName);
            }

            for (Map<String, String> row : rows) {
                row.put("id", UUID.randomUUID().toString());
                row.put("organization_id", masterReq.getOrganizationId());
                row.put("policy_id", masterReq.getPolicyId());
                row.put("fileId", headerId);
                row.put("created_by", uploadedBy);
                row.put("updated_by", uploadedBy);

                dynamoService.insertStagingMasterData(masterTableName, row);
                allInsertedRows.add(new HashMap<>(row));
            }

            // Slice to top 50 for display
            List<Map<String, Object>> top50 = allInsertedRows.stream()
                    .limit(50)
                    .collect(Collectors.toList());

            String message = "Inserted " + allInsertedRows.size() + " rows successfully.";
            return new UploadResult(message, allInsertedRows.size(), top50);

        } catch (Exception e) {
            logger.error("uploadCsvDataToTable exception: {}", e.toString());
            throw new MasterdataServiceException("Upload failed: " + e.getMessage());
        }
    }


	@Override
	public List<Map<String, Object>> getDataByPolicyAndOrgId(SearchRequest searchReq) {
		 
        String tableName = resolveTableNameFromCategory(searchReq.getCategory());
 
        return metadataRepository.getDataByPolicyAndOrgId(tableName, searchReq);
    
	}

	private String resolveTableNameFromCategory(String category) {
         
        return category.toLowerCase();
    }

	@Override
	public List<Map<String, Object>> getAllData(SearchRequest searchReq) {
		 String tableName = resolveTableNameFromCategory(searchReq.getCategory());
		 
	        return metadataRepository.getAllData(tableName, searchReq);
	}

	@Override
	public List<Map<String, Object>> getDataByPolicyId(SearchRequest searchReq) {
		 String tableName = resolveTableNameFromCategory(searchReq.getCategory());
		 
	        return metadataRepository.getDataByPolicyId(tableName, searchReq);
	}

	@Override
	public List<Map<String, Object>> getDataByOrgId(SearchRequest searchReq) {
		 String tableName = resolveTableNameFromCategory(searchReq.getCategory());
		 
	        return metadataRepository.getDataByOrgId(tableName, searchReq);
	}

}
