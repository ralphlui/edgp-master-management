package sg.edu.nus.iss.edgp.masterdata.management.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.api.connector.AdminAPICall;
import sg.edu.nus.iss.edgp.masterdata.management.api.connector.WorkflowAPICall;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.User;

@Component
@RequiredArgsConstructor
public class JSONReader {
	
	private final AdminAPICall adminAPICall;
	private final WorkflowAPICall workflowAPICall;
	
	private static final Logger logger = LoggerFactory.getLogger(JSONReader.class);

	public JSONObject getActiveUserInfo(String userId, String authorizationHeader) {
        JSONObject jsonResponse = new JSONObject();
		String responseStr = adminAPICall.validateActiveUser(userId, authorizationHeader);
		try {
			JSONParser parser = new JSONParser();
			jsonResponse = (JSONObject) parser.parse(responseStr);
			return jsonResponse;
		} catch (ParseException e) {	
			logger.error("Error parsing JSON response for getActiveUserDetails...", e);

		}
		return jsonResponse;
	}
	
	public String getMessageFromResponse(JSONObject jsonResponse) {
		return (String) jsonResponse.get("message");
	}

	public Boolean getSuccessFromResponse(JSONObject jsonResponse) {
		return (Boolean) jsonResponse.get("success");
	}
	
	public User getUserObject(JSONObject userJSONObject) {
		User var = new User();
		JSONObject data = getDataFromResponse(userJSONObject);
		if (data != null) {
			logger.info("User data");
			String userName = GeneralUtility.makeNotNull(data.get("username").toString());
			String email = GeneralUtility.makeNotNull(data.get("email").toString());
			String role = GeneralUtility.makeNotNull(data.get("role").toString());
			String userID = GeneralUtility.makeNotNull(data.get("userID").toString());
			var.setUserId(userID);
			var.setEmail(email);
			var.setRole(role);
			var.setUsername(userName);
		}

		return var;
	}
	

	public JSONObject getDataFromResponse(JSONObject jsonResponse) {
		if (jsonResponse != null && !jsonResponse.isEmpty()) {
			return (JSONObject) jsonResponse.get("data");
		}
		return null;
	}
	
	public JSONObject getLatestFileStatusInfo() {
        JSONObject jsonResponse = new JSONObject();
		String responseStr = workflowAPICall.getLatestFileProcessStatus();
		try {
			JSONParser parser = new JSONParser();
			jsonResponse = (JSONObject) parser.parse(responseStr);
			return jsonResponse;
		} catch (ParseException e) {	
			logger.error("Error parsing JSON response for getLatestFileStatusInfo...", e);

		}
		return jsonResponse;
	}
}
