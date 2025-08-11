package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.Optional;

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;

public interface IHeaderService {

	void saveHeader(String tableName,MasterDataHeader header);
	 	
	Optional<MasterDataHeader> fetchOldestByStage(FileProcessStage stage);
	
	void updateFileStage(String fileId, FileProcessStage processStage);
	
	boolean filenameExists(String filename);
}
