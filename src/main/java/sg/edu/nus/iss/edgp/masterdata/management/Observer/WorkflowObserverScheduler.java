package sg.edu.nus.iss.edgp.masterdata.management.Observer;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;   

@RequiredArgsConstructor
@Service
public class WorkflowObserverScheduler {

	private static final Logger logger = LoggerFactory.getLogger(WorkflowObserverScheduler.class);
 
	private final HeaderService headerService;
	private final MasterdataService masterdataService;

	@Scheduled(fixedDelayString = "PT5M")
	public void checkWorkflowStatusAndPushNext() {
		logger.info("Checking workflow status...");

		try {
			// 1) Get the current PROCESSING file 
			
			Optional<MasterDataHeader> file = headerService.fetchOldestByStage(FileProcessStage.PROCESSING);
			if (file.isEmpty()) {
				logger.info("No processing files found.");
			// (2) Push data to Workflow Queue
				int dispatched = masterdataService.processAndSendRawDataToSqs();
				
				logger.info("Dispatched {} messages to workflow inbound queue.", dispatched);
				
			} else {
				logger.info("File is not complete yet (status={}). Will check again on next poll.",
						  FileProcessStage.PROCESSING);
			}
			
		} catch (MasterdataServiceException e) {

			logger.error("Domain error during workflow polling/publish.", e);
		} catch (Exception e) {

			logger.error("Unexpected error while polling workflow status or pushing next batch.", e);
		}
	}

}
