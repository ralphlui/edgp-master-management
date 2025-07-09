package sg.edu.nus.iss.edgp.masterdata.management.service;
 
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.any;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import io.jsonwebtoken.JwtException;
import sg.edu.nus.iss.edgp.masterdata.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditResponseStatus;
import sg.edu.nus.iss.edgp.masterdata.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.*;

@ExtendWith(MockitoExtension.class)
class AuditServiceTest {

	@Spy
	@InjectMocks
	private AuditService auditService;

	@Mock
	private JWTService jwtService;

	@Mock
	private SQSPublishingService sqsPublishingService;
	
	private static final String userId = "user123";
    private static final String activityType = "Create";
    private static final String activityTypePrefix = "Role:";
    private static final String endpoint = "/api/roles";
    private static final HTTPVerb verb = HTTPVerb.POST;
    private static final String token = "Bearer test.jwt.token";


	@Test
	void testSendMessage_validToken_CallSqsService()
			throws JwtException, IllegalArgumentException, Exception {
		String token = "Bearer valid.jwt.token";
		String extractedUsername = "testuser";

		AuditDTO auditDTO = new AuditDTO();
		auditDTO.setUserId("123");
		auditDTO.setActivityType("TEST_ACTIVITY");

		Mockito.when(jwtService.retrieveUserName("valid.jwt.token")).thenReturn(extractedUsername);

		auditService.sendMessage(auditDTO, token);

		assertEquals(extractedUsername, auditDTO.getUsername());
		verify(sqsPublishingService).sendMessage(auditDTO);
	}

	@Test
	void testSendMessage_emptyJwt_InvalidUsername() {
		String token = "Bearer ";

		AuditDTO auditDTO = new AuditDTO();
		auditDTO.setUserId("456");
		auditDTO.setActivityType("TEST_ACTIVITY");

		auditService.sendMessage(auditDTO, token);

		assertEquals("Invalid Username", auditDTO.getUsername());
		verify(sqsPublishingService).sendMessage(auditDTO);
	}
	
	@Test
    void testSendMessage_shouldHandleExceptionGracefully() throws JwtException, IllegalArgumentException, Exception {
        
        AuditDTO auditDTO = new AuditDTO();

         
        when(jwtService.retrieveUserName("test.jwt.token")).thenReturn("user123");

       
        doThrow(new RuntimeException("SQS failure"))
            .when(sqsPublishingService).sendMessage(any(AuditDTO.class));
 
        assertDoesNotThrow(() -> auditService.sendMessage(auditDTO, token));
 
        verify(jwtService).retrieveUserName("test.jwt.token");
        verify(sqsPublishingService).sendMessage(auditDTO);
    }
	
	 @Test
	    void testCreateAuditDTO_onBuildCorrectDTO() {
	        AuditDTO auditDTO = auditService.createAuditDTO(userId, activityType, activityTypePrefix, endpoint, verb);

	        assertNotNull(auditDTO);
	        assertEquals("Role:Create", auditDTO.getActivityType());
	        assertEquals(userId, auditDTO.getUserId());
	        assertEquals(endpoint, auditDTO.getRequestActionEndpoint());
	        assertEquals(verb, auditDTO.getRequestType());
	    }
	 
	  
	    @Test
	    void testLogAudit_onSuccessStatusAndCallSendMessage() {
	        AuditDTO auditDTO = new AuditDTO();

	        doNothing().when(auditService).sendMessage(any(AuditDTO.class), eq(token));

	        auditService.logAudit(auditDTO, 200, "Audit succeeded", token);

	        assertEquals(200, auditDTO.getStatusCode());
	        assertEquals(AuditResponseStatus.SUCCESS, auditDTO.getResponseStatus());
	        assertEquals("Audit succeeded", auditDTO.getActivityDescription());
	        verify(auditService).sendMessage(auditDTO, token);
	    }

	    @Test
	    void testLogAudit_onFailedStatusAndCallSendMessage() {
	        AuditDTO auditDTO = new AuditDTO();

	        doNothing().when(auditService).sendMessage(any(AuditDTO.class), eq(token));
	        auditService.logAudit(auditDTO, 500, "Audit failed", token);

	        assertEquals(500, auditDTO.getStatusCode());
	        assertEquals(AuditResponseStatus.FAILED, auditDTO.getResponseStatus());
	        assertEquals("Audit failed", auditDTO.getActivityDescription());
	        verify(auditService).sendMessage(auditDTO, token);
	    }


	
}
