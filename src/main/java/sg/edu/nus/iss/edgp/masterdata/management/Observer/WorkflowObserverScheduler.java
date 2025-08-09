package sg.edu.nus.iss.edgp.masterdata.management.Observer;
 
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service; 

import lombok.RequiredArgsConstructor; 
import sg.edu.nus.iss.edgp.masterdata.management.dto.WorkflowStatusResponse;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;

@RequiredArgsConstructor
@Service
public class WorkflowObserverScheduler {

	private static final Logger logger = LoggerFactory.getLogger(WorkflowObserverScheduler.class);	 
	
    private final JSONReader jsonReader;
 
    @Scheduled(fixedRateString = "${polling.interval-ms}")
    public void checkWorkflowStatusAndPushNext() {
        logger.info("Checking workflow status...");

        try {
        	JSONObject jsonObj = jsonReader.getLatestFileStatusInfo();
        	if(jsonObj != null) {
        		WorkflowStatusResponse status = null ;
        		
            logger.info("Latest fileId = {}, status = {} ({} / {})",
            		status.getFileId(), status.getStatus(),
                    status.getProcessedRecords(), status.getTotalRecords());

            if ("COMPLETED".equalsIgnoreCase(status.getStatus())) {
                logger.info("File {} fully processed. Checking for next file to process...", status.getFileId());
  
              //pushRowsToSqs
               

            } else {
            	logger.info("Still processing file: {}", status.getFileId());
            }
        	}

        } catch (Exception e) {
        	logger.error("Error calling Workflow MS status API", e);
        }
    }
}
