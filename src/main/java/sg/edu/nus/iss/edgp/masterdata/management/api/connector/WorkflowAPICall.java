package sg.edu.nus.iss.edgp.masterdata.management.api.connector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class WorkflowAPICall {

	@Value("${workflow.api.url}")
	private String workflowURL;

	private static final Logger logger = LoggerFactory.getLogger(WorkflowAPICall.class);
	 
	public String getFileProcessStatus(String fileId) {
		logger.info("Get Latest File Process Status API is calling ..");
		String responseStr = "";

		try {
			HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

			String url = workflowURL.trim() + "/file/processed";
			logger.info(url);

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(30))
					.header("X-File-Id", fileId)
					.header("Content-Type", "application/json")
					.GET().build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			responseStr = response.body();

			logger.info("Active process detail response: {}", responseStr);

		} catch (Exception e) {
			logger.error("Error occurred in getLatestFileProcessStatus: ", e);
		}

		return responseStr;
	}
}
