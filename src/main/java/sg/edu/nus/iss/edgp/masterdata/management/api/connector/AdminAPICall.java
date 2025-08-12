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
public class AdminAPICall {

	@Value("${admin.api.url}")
	private String adminURL;

	private static final Logger logger = LoggerFactory.getLogger(AdminAPICall.class);
	private static final String GET_SPECIFIC_ACTIVE_USERS_EXCEPTION_MSG = "getSpecificActiveUsers exception occurred";

	public String validateActiveUser(String userId, String authorizationHeader) {
		logger.info("validate active user is calling ..");
		String responseStr = "";

		try {
			HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

			String url = adminURL.trim() + "/users/profile";
		

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(30))
					.header("Authorization", authorizationHeader).header("X-User-Id", userId).header("Content-Type", "application/json")
					.GET().build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			responseStr = response.body();


		} catch (Exception e) {
			logger.error(GET_SPECIFIC_ACTIVE_USERS_EXCEPTION_MSG, e);
		}

		return responseStr;
	}
	
	public String getAccessToken(String email) {
		logger.info("Get access token is calling ..");
		String responseStr = "";

		try {
			HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

			String url = adminURL.trim() + "/users/accessToken";
			

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(30))
					.header("X-User-Email", email).header("Content-Type", "application/json")
					.GET().build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			responseStr = response.body();


		} catch (Exception e) {
			logger.error(GET_SPECIFIC_ACTIVE_USERS_EXCEPTION_MSG, e);
		}

		return responseStr;
	}
}
