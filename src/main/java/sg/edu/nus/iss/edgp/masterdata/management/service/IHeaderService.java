package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;
import java.util.Map;

import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface IHeaderService {

	void saveHeader(String tableName,MasterDataHeader header);
	
	List<Map<String, AttributeValue>> getFileByFileName(String headerTableName, String fileName);
}
