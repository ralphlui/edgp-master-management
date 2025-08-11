package sg.edu.nus.iss.edgp.masterdata.management.Observer;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.WorkflowStatusResponse;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@RequiredArgsConstructor
@Service
public class WorkflowObserverScheduler {

	private static final Logger logger = LoggerFactory.getLogger(WorkflowObserverScheduler.class);
 
	private final HeaderService headerService;
	private final MasterdataService masterdataService;

	@Scheduled(fixedRateString = "${polling.interval-ms}")
	public void checkWorkflowStatusAndPushNext() {
		logger.info("Checking workflow status...");

		try {
			// 1) Get the current PROCESSING file
			Optional<Map<String, AttributeValue>> opt = headerService
					.fetchFileProcessStatus(FileProcessStage.PROCESSING);
			if (opt.isEmpty()) {
				logger.info("No processing files found.");
			// (2) Push data to Workflow Queue
				int dispatched = masterdataService.processAndSendRawDataToSqs();
				
				logger.info("Dispatched {} messages to workflow inbound queue.", dispatched);
				
			} else {
				logger.info("File is not complete yet (status={}). Will check again on next poll.",
						  FileProcessStage.PROCESSING);
			}


			/*Map<String, AttributeValue> fileMap = opt.get();
			String fileId = attrS(fileMap, "id");
			if (fileId == null || fileId.isBlank()) {
				logger.warn("Fetched processing file without a valid 'id' attribute. Skipping.");
				return;
			}

			// 2) Check Workflow MS for latest status of this file
			WorkflowStatusResponse wfStatus = jsonReader.getFileStatus(fileId);
			if (wfStatus == null) {
				logger.warn("Workflow status response is null for fileId={}", fileId);
				return;
			}

			String status = wfStatus.getStatus();
			logger.info("Latest workflow status: fileId={}, status={}", wfStatus.getFileId(), status);

			// 3) If complete ,update file status and push the next data to the queue
			if (isComplete(status)) {
				logger.info("File {} fully processed. Dispatching next batch to SQS...", wfStatus.getFileId());

				// update file status
				headerService.updateFileStatuses(fileId, FileProcessStage.COMPLETE,status);
				
			}*/
			
		} catch (MasterdataServiceException e) {

			logger.error("Domain error during workflow polling/publish.", e);
		} catch (Exception e) {

			logger.error("Unexpected error while polling workflow status or pushing next batch.", e);
		}
	}

	private static String attrS(Map<String, AttributeValue> item, String key) {
		AttributeValue v = item.get(key);
		return (v == null) ? null : v.s();
	}

	private static boolean isComplete(String status) {
		if (status == null)
			return false;
		String s = status.trim();

		return "true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s);
	}

}
