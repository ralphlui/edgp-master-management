package sg.edu.nus.iss.edgp.masterdata.management.aws.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@RequiredArgsConstructor
@Service
public class SQSPublishingService {

	@Value("${aws.sqs.queue.audit.url}")
	String auditQueueURL;
	
	@Value("${aws.sqs.queue.workflow.ingestion.url}")
    String workflowIngestionQueueURL;
	 
	private final SqsClient sqsClient;

	private static final Logger logger = LoggerFactory.getLogger(SQSPublishingService.class);
	
	public void sendMessage(AuditDTO auditDTO) {
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			String messageBody = objectMapper.writeValueAsString(auditDTO);
			byte[] messageBytes = messageBody.getBytes(StandardCharsets.UTF_8);
			int messageSize = messageBytes.length;
			int maxMessageSize = 256 * 1024;

			logger.info("Serialized Audit Log JSON");

			if (messageSize > maxMessageSize) {
				logger.warn("Message size exceeds the 256 KB limit: {} bytes, truncating remarks.", messageSize);

				String truncatedRemarks = truncateMessage(auditDTO.getRemarks(), maxMessageSize, messageBody);
				auditDTO.setRemarks(truncatedRemarks.concat("..."));

				messageBody = objectMapper.writeValueAsString(auditDTO);
				messageBytes = messageBody.getBytes(StandardCharsets.UTF_8);
				logger.info("Truncated message size: {} bytes", messageBytes.length);
			}

			SendMessageRequest sendMsgRequest = SendMessageRequest.builder().queueUrl(auditQueueURL)
					.messageBody(messageBody).delaySeconds(5).build();

			SendMessageResponse response = sqsClient.sendMessage(sendMsgRequest);
			logger.info("Message sent to SQS with message ID: {}", response.messageId());

		} catch (Exception e) {
			logger.error("Error sending message to SQS: {}", e.getMessage(), e);
		}
	}

	public String truncateMessage(String remarks, int maxMessageSize, String currentMessage) {
	    try {
	       
	        byte[] currentMessageBytes = currentMessage.getBytes(StandardCharsets.UTF_8);
	        int currentSize = currentMessageBytes.length;
	        
	        byte[] remarkBytes = remarks.getBytes(StandardCharsets.UTF_8);
	        
	        int remarkSize =remarkBytes.length;

	        int diffMsgSize = currentSize - maxMessageSize;

	        if (diffMsgSize >= remarkSize) {
	            return "";
	        }	      
	        
	        int  allowedBytesForRemarks = remarkSize - (diffMsgSize+5);
	        if (remarkBytes.length <= allowedBytesForRemarks) {
	            return remarks; 
	        }

	        String truncatedRemarks = new String(remarkBytes, 0, allowedBytesForRemarks, StandardCharsets.UTF_8);
	        return truncatedRemarks;
	    } catch (Exception e) {
	        logger.error("Error while truncating message remarks: {}", e.getMessage());
	        return remarks; 
	    }
	}
	
	
	 public void sendRecordToQueue(String messagePayload) {
	        try {
	             
	            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
	                    .queueUrl(workflowIngestionQueueURL)
	                    .messageBody(messagePayload)
	                    .build();

	            SendMessageResponse response = sqsClient.sendMessage(sendMsgRequest);

	             
	            if (response.sdkHttpResponse().isSuccessful()) {
	                System.out.println("Message sent. ID: " + response.messageId());
	            } else {
	                throw new RuntimeException("SQS send failed: " + response.sdkHttpResponse().statusCode());
	            }

	        } catch (Exception e) {
	            throw new RuntimeException("Failed to send message to SQS: " + e.getMessage(), e);
	        }
	    }

}
