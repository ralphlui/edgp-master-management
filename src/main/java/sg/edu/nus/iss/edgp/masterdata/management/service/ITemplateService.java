package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;

import org.springframework.web.multipart.MultipartFile;

import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.TemplateFileFormat;

public interface ITemplateService {
	void createTableFromCsvTemplate(MultipartFile file,String tableName);
	
	List<TemplateFileFormat> parseCsvTemplate(MultipartFile file);
	
    String uploadCsvDataToTable( MultipartFile file,UploadRequest uploadRequest) ;
}
