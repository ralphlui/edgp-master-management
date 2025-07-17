package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface IDynamicDetailService {

	void insertStagingMasterData(String tableName,Map<String, String> rawData);
	
	boolean tableExists(String tableName);
	
	void createTable(String tableName);
	
	void insertValidatedMasterData(String tableName, Map<String, String> rowData);
	
	
	void updateStagingProcessedStatus(String tableName, String id, String newStatus);
	
	List<Map<String, AttributeValue>> getUnprocessedRecordsByFileId(String tableName, String fileId,String uploadedBy);
}
