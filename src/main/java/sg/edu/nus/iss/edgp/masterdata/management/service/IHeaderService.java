package sg.edu.nus.iss.edgp.masterdata.management.service;

public interface IHeaderService {

	void saveHeader(String tableName,String id, String filename, String uploadedBy);
	
	String getFileIdByFileName(String headerTableName, String fileName);
}
