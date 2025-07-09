package sg.edu.nus.iss.edgp.masterdata.management.utility;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import sg.edu.nus.iss.edgp.masterdata.management.api.connector.AdminAPICall;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.User;
 
class JSONReaderTest {

	@Mock
	private AdminAPICall apiCall;

	@InjectMocks
	private JSONReader jsonReader;

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
	}

	@Test
	void testGetActiveUserInfo_validJson() {
		String userId = "123";
		String authHeader = "Bearer token";
		String validJson = "{\"success\":true,\"message\":\"User found\",\"data\":{\"username\":\"john\",\"email\":\"john@example.com\",\"role\":\"ADMIN\",\"userID\":\"123\"}}";

		when(apiCall.validateActiveUser(userId, authHeader)).thenReturn(validJson);

		JSONObject result = jsonReader.getActiveUserInfo(userId, authHeader);

		assertNotNull(result);
		assertTrue((Boolean) result.get("success"));
		assertEquals("User found", result.get("message"));
	}

	@Test
	void testGetActiveUserInfo_invalidJson() {
		String userId = "123";
		String authHeader = "Bearer token";
		String invalidJson = "invalid-json";

		when(apiCall.validateActiveUser(userId, authHeader)).thenReturn(invalidJson);

		JSONObject result = jsonReader.getActiveUserInfo(userId, authHeader);

		assertNotNull(result);
		assertTrue(result.isEmpty());
	}

	@Test
	void testGetMessageFromResponse() {
		JSONObject response = new JSONObject();
		response.put("message", "Operation successful");

		String message = jsonReader.getMessageFromResponse(response);

		assertEquals("Operation successful", message);
	}

	@Test
	void testGetSuccessFromResponse() {
		JSONObject response = new JSONObject();
		response.put("success", true);

		Boolean success = jsonReader.getSuccessFromResponse(response);

		assertTrue(success);
	}

	@Test
	void testGetUserObject() {
		JSONObject data = new JSONObject();
		data.put("username", "alice");
		data.put("email", "alice@example.com");
		data.put("role", "USER");
		data.put("userID", "456");

		JSONObject fullResponse = new JSONObject();
		fullResponse.put("data", data);

		User user = jsonReader.getUserObject(fullResponse);

		assertEquals("alice", user.getUsername());
		assertEquals("alice@example.com", user.getEmail());
		assertEquals("USER", user.getRole());
		assertEquals("456", user.getUserId());
	}

	@Test
	void testGetDataFromResponse_nullOrEmpty() {
		assertNull(jsonReader.getDataFromResponse(null));

		JSONObject empty = new JSONObject();
		assertNull(jsonReader.getDataFromResponse(empty));
	}
}
