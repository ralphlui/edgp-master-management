package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;
import java.util.Map;

import org.springframework.web.multipart.MultipartFile;

import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.TemplateFileFormat;

public interface IMasterdataService {
	 
	List<TemplateFileFormat> parseCsvTemplate(MultipartFile file);
	
	UploadResult uploadCsvDataToTable( MultipartFile file,UploadRequest uploadRequest,String authorizationHeader) ;
    
    List<Map<String, Object>> getDataByPolicyAndOrgId(SearchRequest searchReq) ;
    
    
    List<Map<String, Object>>  getAllData(SearchRequest searchReq) ;
    
   List<Map<String, Object>> getDataByPolicyId(SearchRequest searchReq) ;
   
   List<Map<String, Object>> getDataByOrgId(SearchRequest searchReq) ; 
}
