package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;
import java.util.Map;

import org.springframework.web.multipart.MultipartFile;

import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;

public interface IMasterdataService {

	UploadResult uploadCsvDataToTable(MultipartFile file, UploadRequest uploadRequest, String authorizationHeader);

	List<Map<String, Object>> getAllUploadFiles(String authorizationHeader );
	
	List<Map<String, Object>> getDataByPolicyAndDomainName(SearchRequest searchReq,String authorizationHeader );
	 
	List<Map<String, Object>> getAllData(String authorizationHeader);

	List<Map<String, Object>> getDataByPolicyId(SearchRequest searchReq,String authorizationHeader);
	
	List<Map<String, Object>> getDataByDomainName(SearchRequest searchReq,String authorizationHeader);
	
	List<Map<String, Object>> getDataByFileId(SearchRequest searchReq, String authorizationHeader) ;
	
	int processAndSendRawDataToSqs();
	
	void updateData(UploadRequest uploadRequest,String authorizationHeader);

}
