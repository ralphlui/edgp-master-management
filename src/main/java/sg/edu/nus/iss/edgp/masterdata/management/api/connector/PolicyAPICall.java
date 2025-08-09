package sg.edu.nus.iss.edgp.masterdata.management.api.connector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

public class PolicyAPICall {
	
	@Value("${policy.api.url}")
	private String policyURL;

	private static final Logger logger = LoggerFactory.getLogger(AdminAPICall.class);
	 
	public String getRuleByPolicyId(String policyId, String authorizationHeader) {
		logger.info("Get my-policy API is calling ..");
		String responseStr = "";

		try {
			HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

			String url = policyURL.trim() + "/my-policy";
			logger.info(url);

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(30))
					.header("Authorization", authorizationHeader).header("X-Policy-Id", policyId).header("Content-Type", "application/json")
					.GET().build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			responseStr = response.body();

			logger.info("My-policy  response: {}", responseStr);

		} catch (Exception e) {
			logger.error("Exception is occurred in getRuleByPolicyId ", e);
		}

		return responseStr;
	}

}
