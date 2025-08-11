package sg.edu.nus.iss.edgp.masterdata.management.utility;


import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;

@RequiredArgsConstructor
public class DataUploadValidation {
	
	private final JSONReader jsonReader;
	 
	public ValidationResult isValidToUpload(MultipartFile file, UploadRequest uploadReq,String authHeader) {
		ValidationResult result = new ValidationResult();
		if(file == null || file.isEmpty()) {
			result.setValid(false);
			result.setMessage("File should not be empty.");
			return result;
		}else {
		 String fileName = file.getOriginalFilename();
		 if(fileName == null || fileName.isEmpty()) {
				result.setValid(false);
				result.setMessage("File should not be empty.");
				return result;
			}
		}
		if (uploadReq == null) {
			result.setValid(false);
			result.setMessage("Upload request should not be empty.");
			return result;
			
		}else {
			if(uploadReq.getDomainName().isEmpty() || uploadReq.getDomainName() == null) {
				result.setValid(false);
				result.setMessage("Domain should not be empty.");
				return result;
			}
			if(uploadReq.getOrganizationId().isEmpty() || uploadReq.getOrganizationId() == null) {
				result.setValid(false);
				result.setMessage("Organization should not be empty.");
				return result;
			}
			if(uploadReq.getPolicyId().isEmpty() || uploadReq.getPolicyId() == null) {
				result.setValid(false);
				result.setMessage("Policy should not be empty.");
				return result;
			}else {
				
				PolicyRoot policy=jsonReader.getValidationRules(uploadReq.getPolicyId().trim(), authHeader);
				if(policy == null) {
					result.setValid(false);
					result.setMessage("Policy should not be empty.");
				}
				
				return result;
			}
		}	
		 
	}

}
