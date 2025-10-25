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
public class OrganizationAPICall {

	
	@Value("${organization.api.url}")
	private String orgURL;

	private static final Logger logger = LoggerFactory.getLogger(OrganizationAPICall.class);
	
	public String validateActiveOrganization(String orgId, String authorizationHeader) {
		logger.info("org detail api is calling ..");
		String responseStr = "";

		try {
			HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

			String url = orgURL.trim() + "/my-organization";
			logger.info(url);

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(30))
					.header("Authorization", authorizationHeader).header("X-Org-Id", orgId).header("Content-Type", "application/json")
					.GET().build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			responseStr = response.body();

			logger.info("Active org detail response."+responseStr);

		} catch (Exception e) {
			logger.error("An error occurred while fetching organization data.", e);
		}

		return responseStr;
	}
}

