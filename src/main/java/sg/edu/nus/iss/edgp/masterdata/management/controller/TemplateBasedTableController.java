package sg.edu.nus.iss.edgp.masterdata.management.controller;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.*;
import sg.edu.nus.iss.edgp.masterdata.management.enums.*;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicTableRegistryService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/mdm/tables")
@Validated
public class TemplateBasedTableController {

	@Autowired
    private MasterdataService templateService;
	
	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;
	
	private static final Logger logger = LoggerFactory.getLogger(TemplateBasedTableController.class);	 
	private static final String INVALID_USER_ID = AuditLogInvalidUser.INVALID_USER_ID.toString();	
	private static final String API_ENDPOINT = "/api/mdm/tables";
	private static final String UNEXPECTED_ERROR = "An unexpected error occurred. Please contact support.";
	private static final String LOG_MESSAGE_FORMAT = "{} {}";
	 
	private final AuditService auditService;
	private final DynamicTableRegistryService categoryService;

	
	@GetMapping(value = "/category", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_view:mdm') or hasAuthority('SCOPE_manage:mdm')")
	public ResponseEntity<APIResponse<List<String>>> getAllCategory(
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
			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;
			
	        logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
	        auditDTO.setRemarks(e.getMessage());
	        auditService.logAudit(auditDTO, 500, message, authorizationHeader);
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
	

    @PostMapping(value = "/create", produces = "application/json")
    @PreAuthorize("hasAuthority('SCOPE_manage:mdm')")
    public ResponseEntity<APIResponse<String>>createDynamicTableByCsvTemplate(
    		@RequestHeader("Authorization") String authorizationHeader,@RequestHeader("X-Category") String categoryName,@RequestParam("file") MultipartFile file) {
    	final String activityType = "CreateDynamicTableByCsvTemplate";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT +"/create";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint, httpMethod);

    	try {
    		 boolean exists = templateService.checkTableIfExists(categoryName.toLowerCase());
    	      if(exists) {
    	    	 message ="Table already exists."; 
    	      }else {
            templateService.createTableFromCsvTemplate(file,categoryName.toLowerCase());
            message ="Table created successfully.";
    	      }
            return ResponseEntity.status(HttpStatus.OK).body(APIResponse.successWithNoData( message));
            
        } catch (Exception e) {
            
            message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;
			
	        logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
	        auditDTO.setRemarks(e.getMessage());
	        auditService.logAudit(auditDTO, 500, message, authorizationHeader);
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
    }
}
