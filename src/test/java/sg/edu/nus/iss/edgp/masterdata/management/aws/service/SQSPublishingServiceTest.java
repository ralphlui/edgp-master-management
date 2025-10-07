package sg.edu.nus.iss.edgp.masterdata.management.aws.service; 

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

@ExtendWith(MockitoExtension.class)
class SQSPublishingServiceTest {

    private SqsClient sqsClient;
    private SQSPublishingService svc;

    @BeforeEach
    void setUp() {
        sqsClient = mock(SqsClient.class);
        svc = new SQSPublishingService(sqsClient);

       
        ReflectionTestUtils.setField(svc, "auditQueueURL", "https://sqs.example.com/123/audit");
        ReflectionTestUtils.setField(svc, "workflowIngestionQueueURL", "https://sqs.example.com/123/workflow");
    }
 

    @Test
    void sendMessage_normal_buildsRequestWithQueueAndDelay_andContainsBody() {
        AuditDTO dto = new AuditDTO();
        dto.setRemarks("This is a test message");

        SendMessageResponse mockResp = SendMessageResponse.builder().messageId("1234").build();
        when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(mockResp);

        svc.sendMessage(dto);

        ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient, times(1)).sendMessage(cap.capture());

        SendMessageRequest req = cap.getValue();
        assertEquals("https://sqs.example.com/123/audit", req.queueUrl());
        assertEquals(5, req.delaySeconds().intValue());
        assertTrue(req.messageBody().contains("This is a test message"));
    }

    @Test
    void sendMessage_largeMessage_truncatesRemarks_andRespects256KB() {
        AuditDTO dto = new AuditDTO();
        StringBuilder sb = new StringBuilder(300_000);
        for (int i = 0; i < 300_000; i++) sb.append('a');
        dto.setRemarks(sb.toString());

        SendMessageResponse mockResp = SendMessageResponse.builder().messageId("mid").build();
        when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(mockResp);

        svc.sendMessage(dto);

        ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(cap.capture());

        String body = cap.getValue().messageBody();
        assertTrue(body.getBytes(StandardCharsets.UTF_8).length <= 256 * 1024);
        assertTrue(body.contains("...")); // ellipsis appended after truncation
        assertEquals("https://sqs.example.com/123/audit", cap.getValue().queueUrl());
        assertEquals(5, cap.getValue().delaySeconds().intValue());
    }

    @Test
    void sendMessage_whenClientThrows_isCaught_noExceptionPropagated() {
        AuditDTO dto = new AuditDTO();
        dto.setRemarks("will fail");
        when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                .thenThrow(new RuntimeException("SQS unavailable"));

        assertDoesNotThrow(() -> svc.sendMessage(dto));
    }
 

    @Test
    void truncateMessage_whenDiffExceedsRemarks_returnsEmpty() {
       
        String remarks = "abcdefghij"; 
        String currentMessage = "x".repeat(256 * 1024 + 10 + remarks.length());

        String out = svc.truncateMessage(remarks, 256 * 1024, currentMessage);
        assertEquals("", out);
    }

    @Test
    void truncateMessage_whenCurrentSmallerThanMax_returnsOriginal() {
        String remarks = "short";
        String currentMessage = "x".repeat(1024);
        String out = svc.truncateMessage(remarks, 256 * 1024, currentMessage);
        assertEquals(remarks, out);
    }

    @Test
    void truncateMessage_withMultibyte_remainsWithinAllowedBytes() {
       
        String remarks = "你好世界你好世界";
        String currentMessage = "x".repeat(256 * 1024 + 200);
        String out = svc.truncateMessage(remarks, 256 * 1024, currentMessage);

        assertNotNull(out);
        assertTrue(out.getBytes(StandardCharsets.UTF_8).length <= remarks.getBytes(StandardCharsets.UTF_8).length);
    }


    @Test
    void sendRecordToQueue_success_usesWorkflowQueue() {
        String payload = "{\"k\":\"v\"}";
        SendMessageResponse resp = mock(SendMessageResponse.class);
        SdkHttpResponse http = SdkHttpResponse.builder().statusCode(200).build();

        when(resp.sdkHttpResponse()).thenReturn(http);
        when(resp.messageId()).thenReturn("m-1");
        when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(resp);

        assertDoesNotThrow(() -> svc.sendRecordToQueue(payload));

        ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(cap.capture());
        SendMessageRequest req = cap.getValue();
        assertEquals("https://sqs.example.com/123/workflow", req.queueUrl());
        assertEquals(payload, req.messageBody());
    }

    @Test
    void sendRecordToQueue_whenNotSuccessful_throwsRuntimeException() {
        String payload = "{\"x\":1}";
        SendMessageResponse resp = mock(SendMessageResponse.class);
        SdkHttpResponse http = SdkHttpResponse.builder().statusCode(500).build();

        when(resp.sdkHttpResponse()).thenReturn(http);
        when(sqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(resp);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> svc.sendRecordToQueue(payload));
        assertTrue(ex.getMessage().contains("SQS send failed") || ex.getMessage().contains("Raw Data SQS send failed"));
    }

    @Test
    void sendRecordToQueue_whenClientThrows_wrapsInRuntimeException() {
        when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                .thenThrow(new RuntimeException("network"));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> svc.sendRecordToQueue("{\"a\":1}"));
        assertTrue(ex.getMessage().startsWith("Failed to send Raw Data message"));
        assertNotNull(ex.getCause());
    }
}

