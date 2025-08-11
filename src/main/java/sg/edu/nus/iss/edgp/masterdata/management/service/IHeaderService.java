package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.Map;
import java.util.Optional;

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface IHeaderService {

	void saveHeader(String tableName,MasterDataHeader header);
	 	
	Optional<Map<String, AttributeValue>> fetchFileProcessStatus(FileProcessStage processStage);
	
	void updateFileStage(String fileId, FileProcessStage processStage);
}
