package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;

@RequiredArgsConstructor
@Component
public class DataUploadValidation {
	
	@Value("${aws.dynamodb.table.master.data.header}")
	private String masterDataHeader;

	private final JSONReader jsonReader;
	private final HeaderService headerService;
	private final DynamicDetailService dynamoService;

	public ValidationResult isValidToUpload(MultipartFile file, UploadRequest uploadReq, String authHeader) {
		ValidationResult result = new ValidationResult();
		if (file == null || file.isEmpty()) {
			result.setValid(false);
			result.setMessage("File is required.");
			result.setStatus(HttpStatus.BAD_REQUEST);
			return result;
		} else {
			String fileName = file.getOriginalFilename();
			if (fileName == null || fileName.isEmpty()) {
				result.setValid(false);
				result.setMessage("File is required.");
				result.setStatus(HttpStatus.BAD_REQUEST);
				return result;
			} else {
				if (dynamoService.tableExists(masterDataHeader.trim())) {

					boolean isExists = headerService.filenameExists(fileName.trim());
					if (isExists) {
						result.setValid(false);
						result.setMessage("A file named " + fileName + " already exists. Choose a different name");
						result.setStatus(HttpStatus.CONFLICT);
						return result;
					}
				}
			}

		}
		if (uploadReq == null) {
			result.setValid(false);
			result.setMessage("Upload request is required.");
			result.setStatus(HttpStatus.BAD_REQUEST);
			return result;

		} else {
			if (uploadReq.getDomainName().isEmpty() || uploadReq.getDomainName() == null) {
				result.setValid(false);
				result.setMessage("Domain is required.");
				result.setStatus(HttpStatus.BAD_REQUEST);
				return result;
			}
			
			if (uploadReq.getPolicyId().isEmpty() || uploadReq.getPolicyId() == null) {
				result.setValid(false);
				result.setMessage("Policy is required.");
				result.setStatus(HttpStatus.BAD_REQUEST);
				return result;
			} else {

				PolicyRoot policy = jsonReader.getValidationRules(uploadReq.getPolicyId().trim(), authHeader);
				if (policy == null) {
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
	public ValidationResult isValidToUpdate(Map<String, Object> data) {
		ValidationResult result = new ValidationResult();
		
		if (!data.containsKey("id")) {
			result.setValid(false);
			result.setMessage("Missing 'id' field in request.");
			result.setStatus(HttpStatus.BAD_REQUEST);
			return result;
			 
		}

		
		if(data.isEmpty() || data == null) {
			result.setValid(false);
			result.setMessage("Update data is required.");
			result.setStatus(HttpStatus.BAD_REQUEST);
			return result;
		}
		
		String id = String.valueOf(data.get("id"));
		
		if(id.isEmpty() || id == null) {
			result.setValid(false);
			result.setMessage("Id is required.");
			result.setStatus(HttpStatus.BAD_REQUEST);
			return result;
		}
		 
		
		result.setValid(true);
		result.setStatus(HttpStatus.OK);
		return result;
	}

}
