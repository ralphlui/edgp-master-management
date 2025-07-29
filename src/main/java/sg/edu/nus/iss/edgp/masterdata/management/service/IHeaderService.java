package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface IHeaderService {

	void saveHeader(String tableName,String id, String filename, String uploadedBy, int totalRows);
	
	List<Map<String, AttributeValue>> getFileByFileName(String headerTableName, String fileName);
}
