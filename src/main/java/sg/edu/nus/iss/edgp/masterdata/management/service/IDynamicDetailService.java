package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.Map;

public interface IDynamicDetailService {

	void insertStagingMasterData(String tableName,Map<String, String> rawData);
	
	boolean tableExists(String tableName);
	
	void createTable(String tableName);
}
