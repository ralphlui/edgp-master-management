package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map; 
import java.util.Set;

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
 
	    if (file == null || file.isEmpty() || file.getOriginalFilename() == null || file.getOriginalFilename().isEmpty()) {
	        result.setValid(false);
	        result.setMessage("File is required.");
	        result.setStatus(HttpStatus.BAD_REQUEST);
	        return result;
	    }

	    String fileName = file.getOriginalFilename();
	    if (dynamoService.tableExists(masterDataHeader.trim())) {
	        if (headerService.filenameExists(fileName.trim())) {
	            result.setValid(false);
	            result.setMessage("A file named " + fileName.trim() + " already exists. Choose a different name");
	            result.setStatus(HttpStatus.CONFLICT);
	            return result;
	        }
	    }
 
	    if (uploadReq == null) {
	        result.setValid(false);
	        result.setMessage("Upload request is required.");
	        result.setStatus(HttpStatus.BAD_REQUEST);
	        return result;
	    }
 
	    if (uploadReq.getDomainName() == null || uploadReq.getDomainName().isEmpty()) {
	        result.setValid(false);
	        result.setMessage("Domain is required.");
	        result.setStatus(HttpStatus.BAD_REQUEST);
	        return result;
	    }

	    if (uploadReq.getPolicyId() == null || uploadReq.getPolicyId().isEmpty()) {
	        result.setValid(false);
	        result.setMessage("Policy is required.");
	        result.setStatus(HttpStatus.BAD_REQUEST);
	        return result;
	    }

	    PolicyRoot policy = jsonReader.getValidationRules(uploadReq.getPolicyId().trim(), authHeader);
	    if (policy == null) {
	        result.setValid(false);
	        result.setMessage("Policy is invalid.");
	        result.setStatus(HttpStatus.BAD_REQUEST);
	        return result;
	    }
 
	    result.setValid(true);
	    result.setStatus(HttpStatus.OK);
	    result.setMessage("OK");
	    return result;
	}


	

	public ValidationResult isValidToUpsert(Map<String, Object> request, boolean create) {
	    ValidationResult res = new ValidationResult();
	    Map<String, Object> data = unwrapData(request);
	    if (data.isEmpty()) return fail(new ValidationResult(), "Data payload is required.", HttpStatus.BAD_REQUEST);

	    if (data == null || data.isEmpty()) {
	        return fail(res, "Data payload is required.", HttpStatus.BAD_REQUEST);
	    }

	    // Extract trimmed strings
	    String id         = str(data.get("id"));
	    String policyId   = str(data.get("policy_id"));
	    String domainName = str(data.get("domain_name"));

	    // id required only for update
	    if (!create && isBlank(id)) {
	        return fail(res, "Missing or empty 'id'.", HttpStatus.BAD_REQUEST);
	    }

	    List<String> missing = new ArrayList<>();
	    if (isBlank(policyId))   missing.add("policy_id");
	    if (isBlank(domainName)) missing.add("domain_name");

	    if (!missing.isEmpty()) {
	        return fail(res, "Missing or empty: " + String.join(", ", missing), HttpStatus.BAD_REQUEST);
	    }

	    if (!create) {
	        Set<String> meta = Set.of("id", "policy_id", "domain_name");
	        boolean hasUpdatable = data.entrySet().stream()
	                .anyMatch(e -> e.getKey() != null
	                        && !meta.contains(e.getKey().trim())
	                        && e.getValue() != null);
	        if (!hasUpdatable) {
	            return fail(res, "No updatable fields provided.", HttpStatus.BAD_REQUEST);
	        }
	    }

	    res.setValid(true);
	    res.setStatus(HttpStatus.OK);
	    res.setMessage("OK");
	    return res;
	}

	private static String str(Object o) {
	    return (o == null) ? "" : o.toString().trim();
	}
	private static boolean isBlank(String s) {
	    return s == null || s.isBlank();
	}
	private static ValidationResult fail(ValidationResult r, String msg, HttpStatus status) {
	    r.setValid(false);
	    r.setMessage(msg);
	    r.setStatus(status);
	    return r;
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> unwrapData(Map<String, Object> request) {
	    if (request == null) return java.util.Collections.emptyMap();

	    Object data = request.get("data");
	    if (data instanceof Map<?, ?> m) {
	        return (Map<String, Object>) m;
	    }
	    if (data instanceof String s) {
	        // If someone sent "data" as a JSON string, try parse it
	        try {
	            return new com.fasterxml.jackson.databind.ObjectMapper().readValue(
	                s, new com.fasterxml.jackson.core.type.TypeReference<Map<String,Object>>() {});
	        } catch (Exception ignore) { /* fall through to return request */ }
	    }

	    return request;
	}

	
}
