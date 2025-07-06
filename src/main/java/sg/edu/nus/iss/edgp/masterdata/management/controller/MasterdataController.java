package sg.edu.nus.iss.edgp.masterdata.management.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.APIResponse;
import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.masterdata.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.masterdata.management.exception.DynamicTableRegistryServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicTableRegistryService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.TemplateService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/mdm")
@Validated
public class MasterdataController {

	private final TemplateService templateService;
	private final AuditService auditService;

	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;

	private static final Logger logger = LoggerFactory.getLogger(DynamicTableRegistryController.class);
	private static final String INVALID_USER_ID = AuditLogInvalidUser.INVALID_USER_ID.toString();
	private static final String API_ENDPOINT = "/api/mdm";
	private static final String UNEXPECTED_ERROR = "An unexpected error occurred. Please contact support.";
	private static final String LOG_MESSAGE_FORMAT = "{} {}";

	@PostMapping(value = "/upload", produces = "application/json")
    public ResponseEntity<APIResponse<String>>uploadAndInsertCsvData(
			@RequestHeader("Authorization") String authorizationHeader,
			@RequestPart("UploadRequest") UploadRequest uploadReq,
			@RequestParam("file") MultipartFile file) {

		final String activityType = "Upload Master Data";

		final HTTPVerb httpMethod = HTTPVerb.POST;
		String message = "";
		String endPoint = API_ENDPOINT + "/upload";
		AuditDTO auditDTO = auditService.createAuditDTO(INVALID_USER_ID, activityType, activityTypePrefix, endPoint,
				httpMethod);

		try {
			message = templateService.uploadCsvDataToTable(file,uploadReq);
			if(message.equals("")) {
				message="Upload failed due to incorrect column format or missing values.";
				auditService.logAudit(auditDTO, 500, "", "");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(APIResponse.error(message));
               
			}
			return ResponseEntity.status(HttpStatus.OK).body(APIResponse.successWithNoData(message));
		} catch (Exception e) {

			message = e instanceof DynamicTableRegistryServiceException ? e.getMessage() : UNEXPECTED_ERROR;

			logger.error(LOG_MESSAGE_FORMAT, message, e.getMessage());
			auditDTO.setRemarks(e.getMessage());
			auditService.logAudit(auditDTO, 500, message, authorizationHeader);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(APIResponse.error(message));
		}
	}

}
