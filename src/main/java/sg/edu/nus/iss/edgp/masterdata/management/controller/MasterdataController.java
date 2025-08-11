package sg.edu.nus.iss.edgp.masterdata.management.controller;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.*;
import sg.edu.nus.iss.edgp.masterdata.management.enums.*;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/mdm/data")
@Validated
public class MasterdataController {

	private final MasterdataService masterdataService;
	private final AuditService auditService;
	

	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;

	private static final Logger logger = LoggerFactory.getLogger(MasterdataController.class);
	private static final String INVALID_USER_ID = AuditLogInvalidUser.INVALID_USER_ID.toString();
	private static final String API_ENDPOINT = "/api/mdm/data";
	private static final String UNEXPECTED_ERROR = "An unexpected error occurred. Please contact support.";
	private static final String LOG_MESSAGE_FORMAT = "{} {}";

	@PostMapping(value = "/upload", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_manage:mdm')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>>  uploadAndInsertCsvData(
			@RequestHeader("Authorization") String authorizationHeader,
			@RequestPart("UploadRequest") UploadRequest uploadReq, @RequestParam("file") MultipartFile file) {

		final String activityType = "Upload Master Data";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT + "/upload";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint,
				httpMethod);

		try {
			 
			UploadResult result = masterdataService.uploadCsvDataToTable(file, uploadReq,authorizationHeader);
			if (result.getTotalRecord()< 1) {
				message = "Upload failed due to incorrect column format or missing values.";
				auditService.logAudit(auditDTO, 500, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.NOT_FOUND).body(APIResponse.error(message));

			}
			
			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(result.getData(), result.getMessage(), result.getTotalRecord()));
		} catch (Exception e) {

			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
	

	@GetMapping(value = "", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_view:mdm') or hasAuthority('SCOPE_manage:mdm')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> getUploadedData(
			@RequestHeader("Authorization") String authorizationHeader,
			@Valid @ModelAttribute SearchRequest searchRequest) {

		final String activityType = "Get All Uploaded Data" + searchRequest.getCategory() + "List";
		final HTTPVerb httpMethod = HTTPVerb.GET;
		final String endpoint = API_ENDPOINT;

		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endpoint,
				httpMethod);

		try {
			
		    String policyId = searchRequest.getPolicyId();
		    String orgId = searchRequest.getOrganizationId();

		    boolean hasPolicyId = policyId != null && !policyId.isBlank();
		    boolean hasOrgId = orgId != null && !orgId.isBlank();
		    List<Map<String, Object>> result;
			
			if (hasPolicyId && hasOrgId) {
				result= masterdataService.getDataByPolicyAndOrgId( searchRequest);
		    } else if (hasPolicyId) {
		    	result= masterdataService.getDataByPolicyId(searchRequest);
		    } else if (hasOrgId) {
		    	result= masterdataService.getDataByOrgId(searchRequest);
		    } else {
		    	result= masterdataService.getAllData(searchRequest);
		    }
			 
			String message = result.isEmpty() ? "No data found." : "Successfully retrieved "+searchRequest.getCategory()+" data.";
			auditService.logAudit(auditDTO, 200, message, authorizationHeader);

			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(result, message, result.size()));

		} catch (Exception e) {
			String errorMessage = (e instanceof MasterdataServiceException) ? e.getMessage()
					: UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, errorMessage, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, errorMessage, authorizationHeader);

			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(errorMessage));
		}
	}

}
