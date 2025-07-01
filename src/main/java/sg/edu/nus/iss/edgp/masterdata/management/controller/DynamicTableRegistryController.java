package sg.edu.nus.iss.edgp.masterdata.management.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor; 
import sg.edu.nus.iss.edgp.masterdata.management.dto.*;
import sg.edu.nus.iss.edgp.masterdata.management.enums.*;
import sg.edu.nus.iss.edgp.masterdata.management.exception.DynamicTableRegistryServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicTableRegistryService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/mdm")
@Validated
public class DynamicTableRegistryController {
	
	private static final Logger logger = LoggerFactory.getLogger(DynamicTableRegistryController.class);	 
	private static final String INVALID_USER_ID = AuditLogInvalidUser.INVALID_USER_ID.toString();	
	private static final String API_ENDPOINT = "/api/mdm";
	private static final String UNEXPECTED_ERROR = "An unexpected error occurred. Please contact support.";
	private static final String LOG_MESSAGE_FORMAT = "{} {}";
	 
	 
	private final AuditService auditService;
	private final DynamicTableRegistryService categoryService;

	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;
	

	@GetMapping(value = "/categories", produces = "application/json")
	public ResponseEntity<APIResponse<List<String>>> getAllActiveRoleList(
			@RequestHeader("Authorization") String authorizationHeader) {

		final String activityType = "GetCategoryList";

		final HTTPVerb httpMethod = HTTPVerb.GET;
		String message = "";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, API_ENDPOINT, httpMethod);

		try {
			
			List<String> categories = categoryService.findCategories();

			if (!categories.isEmpty()) {
				message = "Successfully retrieved all category.";
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(categories, message,categories.size()));
				
			} else {
				message = "No data found.";
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(categories, message,categories.size()));
				
			}

		} catch (Exception e) {
			message = e instanceof DynamicTableRegistryServiceException ? e.getMessage() : UNEXPECTED_ERROR;
			
	        logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
	        auditDTO.setRemarks(e.getMessage());
	        auditService.logAudit(auditDTO, 500, message, authorizationHeader);
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
}
