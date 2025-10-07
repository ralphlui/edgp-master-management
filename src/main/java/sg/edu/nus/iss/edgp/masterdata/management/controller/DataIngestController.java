package sg.edu.nus.iss.edgp.masterdata.management.controller;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.APIResponse;
import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.masterdata.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.DataIngestResult;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DataIngestService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DomainService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DataUploadValidation;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/mdm/data/ingest")
@Validated
public class DataIngestController {

	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;

	private static final Logger logger = LoggerFactory.getLogger(MasterdataController.class);
	private static final String INVALID_USER_ID = AuditLogInvalidUser.INVALID_USER_ID.toString();
	private static final String API_ENDPOINT = "/api/data";
	private static final String UNEXPECTED_ERROR = "An unexpected error occurred. Please contact support.";
	private static final String LOG_MESSAGE_FORMAT = "{} {}";

	private final AuditService auditService;
	private final DataUploadValidation dataUploadValidation;
	private final DataIngestService dataIngestService;

	@PostMapping(value = "", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_view:policy')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> createData(
			@RequestHeader("Authorization") String authorizationHeader,@RequestBody Map<String, Object> data) {

		final String activityType = "Update Master Data";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT + "/update";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint,
				httpMethod);

		try {
			ValidationResult validResult = dataUploadValidation.isValidToUpsert(data,true);
			if(!validResult.isValid()) {
				message = validResult.getMessage();
				auditService.logAudit(auditDTO,  400, message, authorizationHeader);
				return ResponseEntity.status(validResult.getStatus()).body(APIResponse.error(message));

			}


			UploadResult result = dataIngestService.processIngest(data, authorizationHeader);
			if (result.getTotalRecord() < 1) {
				message = result.getMessage();
				auditService.logAudit(auditDTO, 500, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));

			}

			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(result.getData(), result.getMessage(), result.getTotalRecord()));
		}  catch (Exception e) {

			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}

	@PutMapping(value = "/update", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_view:policy')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> updateData(
			@RequestHeader("Authorization") String authorizationHeader, @RequestBody Map<String, Object> data) {

		final String activityType = "Update Master Data";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT + "/update";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint,
				httpMethod);

		try {
 
			
			ValidationResult validResult = dataUploadValidation.isValidToUpsert(data,false);
			if(!validResult.isValid()) {
				message = validResult.getMessage();
				auditService.logAudit(auditDTO,  400, message, authorizationHeader);
				return ResponseEntity.status(validResult.getStatus()).body(APIResponse.error(message));

			}

			UploadResult result = dataIngestService.updateDataToTable(data, authorizationHeader);
			if (result.getTotalRecord() < 1) {
				message = result.getMessage();
				auditService.logAudit(auditDTO, 500, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));

			}

			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.success(result.getData(), result.getMessage(), result.getTotalRecord()));
		}  catch (Exception e) {

			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
}
