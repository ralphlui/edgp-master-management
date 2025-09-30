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

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.*;
import sg.edu.nus.iss.edgp.masterdata.management.enums.*;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.Domain;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DomainService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/mdm/tables")
@Validated
public class DomainController {

	
	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;
	
	private static final Logger logger = LoggerFactory.getLogger(DomainController.class);	 
	private static final String INVALID_USER_ID = AuditLogInvalidUser.INVALID_USER_ID.toString();	
	private static final String API_ENDPOINT = "/api/mdm/tables";
	private static final String UNEXPECTED_ERROR = "An unexpected error occurred. Please contact support.";
	private static final String LOG_MESSAGE_FORMAT = "{} {}";
	 
	private final AuditService auditService;
	private final DomainService domainService;

	
	@GetMapping(value = "/domains", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_view:mdm') or hasAuthority('SCOPE_manage:mdm')")
	public ResponseEntity<APIResponse<List<String>>> getDomains(
			@RequestHeader("Authorization") String authorizationHeader) {

		final String activityType = "GetDomainList";

		final HTTPVerb httpMethod = HTTPVerb.GET;
		String message = "";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, API_ENDPOINT, httpMethod);

		try {
			
			List<String> categories = domainService.findDomains();

			if (!categories.isEmpty()) {
				message = "Successfully retrieved all domain.";
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(categories, message,categories.size()));
				
			} else {
				message = "No data found.";
				auditService.logAudit(auditDTO, 200, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(categories, message,categories.size()));
				
			}

		} catch (Exception e) {
			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;
			
	        logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
	        auditDTO.setRemarks(e.getMessage());
	        auditService.logAudit(auditDTO, 500, message, authorizationHeader);
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
	
	@PostMapping(value = "domain/create", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_view:mdm') or hasAuthority('SCOPE_manage:mdm')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> createDomain(
			@RequestHeader("Authorization") String authorizationHeader,@RequestBody Domain domain) {

		final String activityType = "Create Domain";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT + "/domain/create";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint,
				httpMethod);

		try {
			
			String domainName = domain.getName() == null ? "" : domain.getName().trim();
	        if (domainName.isEmpty()) {
	             message = "Domain Name is required";
	            auditService.logAudit(auditDTO, 400, message, authorizationHeader);
	            return ResponseEntity.status(400).body(APIResponse.error(message));
	        }



			boolean isCreated=domainService.createDomain(domainName);
			if (!isCreated) {
				message = "Domain creation failed";
				auditService.logAudit(auditDTO, 500, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));

			}

			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.successWithNoData( "Domain successfully created "));
		}  catch (Exception e) {

			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
	
}
