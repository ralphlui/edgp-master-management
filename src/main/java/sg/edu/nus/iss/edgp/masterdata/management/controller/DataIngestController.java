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
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.APIResponse;
import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.masterdata.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.DataIngestResult;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DataIngestService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DomainService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DataUploadValidation;
import sg.edu.nus.iss.edgp.masterdata.management.utility.IngestRequest;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/data/ingest/")
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
	private final DomainService domainService;
	private final DataUploadValidation dataUploadValidation;
	private final DataIngestService dataIngestService;

	@PutMapping(value = "/create", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_manage:mdm')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> createData(
			@RequestHeader("Authorization") String authorizationHeader, @Valid @RequestBody IngestRequest request) {

		final String activityType = "Update Master Data";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT + "/update";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint,
				httpMethod);

		try {

			if (request.items().isEmpty()) {
				message = "PayLoad is required.";
				auditService.logAudit(auditDTO, 400, message, authorizationHeader);
				return ResponseEntity.status(400).body(APIResponse.error(message));

			}
			DataIngestResult result = dataIngestService.processIngest(request,authorizationHeader);
			if (result.getTotalRecord() < 1) {
				message = result.getMessage();
				auditService.logAudit(auditDTO, 500, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));

			}

			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.successWithNoData(result.getMessage()));
		} catch (Exception e) {

			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}

	@PutMapping(value = "/update", produces = "application/json")
	@PreAuthorize("hasAuthority('SCOPE_manage:mdm') or hasAuthority('SCOPE_manage:mdm')")
	public ResponseEntity<APIResponse<List<Map<String, Object>>>> updateData(
			@RequestHeader("Authorization") String authorizationHeader, @Valid @RequestBody IngestRequest request) {

		final String activityType = "Update Master Data";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT + "/update";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint,
				httpMethod);

		try {
			if (request.items().isEmpty()) {
				message = "PayLoad is required.";
				auditService.logAudit(auditDTO, 400, message, authorizationHeader);
				return ResponseEntity.status(400).body(APIResponse.error(message));

			}
			DataIngestResult result = dataIngestService.processIngest(request,authorizationHeader);
			if (result.getTotalRecord() < 1) {
				message = result.getMessage();
				auditService.logAudit(auditDTO, 500, message, authorizationHeader);
				return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));

			}

			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.successWithNoData(result.getMessage()));
		} catch (Exception e) {

			message = e instanceof MasterdataServiceException ? e.getMessage() : UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}
}
