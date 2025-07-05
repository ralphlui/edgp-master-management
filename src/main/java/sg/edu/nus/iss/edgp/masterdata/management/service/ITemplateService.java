package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;

import org.springframework.web.multipart.MultipartFile;

import sg.edu.nus.iss.edgp.masterdata.management.pojo.CSVFormat;

public interface ITemplateService {
	void createTableFromCsvTemplate(MultipartFile file,String tableName);
	
	List<CSVFormat> parseCsvTemplate(MultipartFile file);

}
