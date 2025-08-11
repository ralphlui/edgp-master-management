package sg.edu.nus.iss.edgp.masterdata.management.utility;


import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;

@RequiredArgsConstructor
public class DataUploadValidation {
	
	private final JSONReader jsonReader;
	private final HeaderService headerService;
	 
	public ValidationResult isValidToUpload(MultipartFile file, UploadRequest uploadReq,String authHeader) {
		ValidationResult result = new ValidationResult();
		if(file == null || file.isEmpty()) {
			result.setValid(false);
			result.setMessage("File is required.");
			result.setStatus(HttpStatus.BAD_REQUEST);
			return result;
		}else {
		 String fileName = file.getOriginalFilename();
		 if(fileName == null || fileName.isEmpty()) {
				result.setValid(false);
				result.setMessage("File is required.");
				result.setStatus(HttpStatus.BAD_REQUEST);
				return result;
			}else {
				boolean isExists = headerService.filenameExists(fileName.trim());
				if(isExists) {
					result.setValid(false);
					result.setMessage("A file named "+fileName+" already exists. Choose a different name");
					result.setStatus(HttpStatus.CONFLICT);
					return result;
				}
			}
		 
		}
		if (uploadReq == null) {
			result.setValid(false);
			result.setMessage("Upload request is required.");
			result.setStatus(HttpStatus.BAD_REQUEST);
			return result;
			
		}else {
			if(uploadReq.getDomainName().isEmpty() || uploadReq.getDomainName() == null) {
				result.setValid(false);
				result.setMessage("Domain is required.");
				result.setStatus(HttpStatus.BAD_REQUEST);
				return result;
			}
			if(uploadReq.getOrganizationId().isEmpty() || uploadReq.getOrganizationId() == null) {
				result.setValid(false);
				result.setMessage("Organization is required.");
				result.setStatus(HttpStatus.BAD_REQUEST);
				return result;
			}
			if(uploadReq.getPolicyId().isEmpty() || uploadReq.getPolicyId() == null) {
				result.setValid(false);
				result.setMessage("Policy is required.");
				result.setStatus(HttpStatus.BAD_REQUEST);
				return result;
			}else {
				
				PolicyRoot policy=jsonReader.getValidationRules(uploadReq.getPolicyId().trim(), authHeader);
				if(policy == null) {
					result.setValid(false);
					result.setMessage("Policy is invalid.");
					result.setStatus(HttpStatus.BAD_REQUEST);
				}
				result.setValid(true);
				result.setStatus(HttpStatus.OK);
				return result;
			}
		}	
		 
	}

}
