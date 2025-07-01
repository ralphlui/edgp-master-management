package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.enums.*;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JwtService;
import sg.edu.nus.iss.edgp.masterdata.management.service.IAuditService;

@RequiredArgsConstructor
@Service
public class AuditService implements IAuditService {

	private static final Logger logger = LoggerFactory.getLogger(AuditService.class);

	private final JwtService jwtService;
	
	private final SQSPublishingService sqsPublishingService;

	@Override
	public void sendMessage(AuditDTO autAuditDTO,String token) {

		try {
			String userName = "Invalid Username";
			String userID="";

			if (!token.isEmpty()) {
				
				String jwtToken = token.substring(7);
				
				
			   userName = Optional.ofNullable(jwtService.retrieveUserName(jwtToken))
		                   .orElse("Invalid Username");
			   autAuditDTO.setUsername(userName);
			   userID = jwtService.extractUserIdAllowExpiredToken(jwtToken);

			}
			
			if(autAuditDTO.getUsername().equals("")) {
				autAuditDTO.setUsername("Invalid UserName");
				autAuditDTO.setUserId(userID);
			}

			sqsPublishingService.sendMessage(autAuditDTO);

		} catch (Exception e) {
			logger.error("Error sending generateMessage to SQS: {}", e);
		}

	}

	public AuditDTO createAuditDTO(String userId,String activityType, String activityTypePrefix, String endpoint,
			HTTPVerb verb) {
		AuditDTO auditDTO = new AuditDTO();
		auditDTO.setActivityType(activityTypePrefix.trim() + activityType);
		auditDTO.setUserId(userId);
		auditDTO.setRequestType(verb);
		auditDTO.setRequestActionEndpoint(endpoint);
		return auditDTO;
	}

	public void logAudit(AuditDTO auditDTO, int stausCode, String message,String token) {
		logger.error(message);
		auditDTO.setStatusCode(stausCode);
		if (stausCode == 200) {
			auditDTO.setResponseStatus(AuditResponseStatus.SUCCESS);

		} else {
			auditDTO.setResponseStatus(AuditResponseStatus.FAILED);
		}
		auditDTO.setActivityDescription(message);
		this.sendMessage(auditDTO,token);

	}

}
