package sg.edu.nus.iss.edgp.masterdata.management.aws.service; 

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;

import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
 
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;

@ExtendWith(MockitoExtension.class)
public class SQSPublishingServiceTest {

	private SqsClient sqsClient;
	private SQSPublishingService publishingService;

	@BeforeEach
	void setUp() {
		sqsClient = mock(SqsClient.class);
		publishingService = new SQSPublishingService(sqsClient);
	}

	@Test
	void testSendMessage_NormalCase() throws Exception {
		AuditDTO auditDTO = new AuditDTO();
		auditDTO.setRemarks("This is a test message");

		SendMessageResponse mockResponse = SendMessageResponse.builder().messageId("1234").build();
		when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(mockResponse);

		publishingService.sendMessage(auditDTO);

		ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(sqsClient, times(1)).sendMessage(captor.capture());

		SendMessageRequest actualRequest = captor.getValue();
		assertTrue(actualRequest.messageBody().contains("This is a test message"));
	}

	@Test
	void testSendMessage_TooLargeMessage_TruncatesRemarks() throws Exception {
		AuditDTO auditDTO = new AuditDTO();
		StringBuilder longRemarks = new StringBuilder();
		for (int i = 0; i < 300_000; i++) {
			longRemarks.append("a");
		}
		auditDTO.setRemarks(longRemarks.toString());

		SendMessageResponse mockResponse = SendMessageResponse.builder().messageId("999").build();
		when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(mockResponse);

		publishingService.sendMessage(auditDTO);

		ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
		verify(sqsClient, times(1)).sendMessage(captor.capture());

		String finalMessage = captor.getValue().messageBody();
		byte[] messageBytes = finalMessage.getBytes(StandardCharsets.UTF_8);
		assertTrue(messageBytes.length <= 256 * 1024);
		assertTrue(finalMessage.contains("..."));
	}

	@Test
	void testSendMessage_WhenExceptionThrown_LogsError() {
		AuditDTO auditDTO = new AuditDTO();
		auditDTO.setRemarks("will fail");

		when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenThrow(new RuntimeException("SQS unavailable"));

		assertDoesNotThrow(() -> publishingService.sendMessage(auditDTO));
	}

    
}

