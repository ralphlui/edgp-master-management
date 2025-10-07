package sg.edu.nus.iss.edgp.masterdata.management.utility;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.api.connector.AdminAPICall;
import sg.edu.nus.iss.edgp.masterdata.management.api.connector.OrganizationAPICall;
import sg.edu.nus.iss.edgp.masterdata.management.api.connector.PolicyAPICall;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.User;

@ExtendWith(MockitoExtension.class)
class JSONReaderTest {

    @Mock private AdminAPICall adminAPICall;
    @Mock private PolicyAPICall policyAPICall;
    @Mock private OrganizationAPICall orgAPICall;
    @Mock private JSONDataMapper mapper;

    @InjectMocks
    private JSONReader jsonReader;

    @BeforeEach
    void init() {
      
    }

    

    @Test
    void getActiveUserInfo_validJson_parsesAndReturnsObject() {
        String userId = "123";
        String authHeader = "Bearer token";
        String validJson =
                "{\"success\":true,\"message\":\"User found\",\"data\":{" +
                "\"username\":\"john\",\"email\":\"john@example.com\",\"role\":\"ADMIN\",\"userID\":\"123\"}}";

        when(adminAPICall.validateActiveUser(userId, authHeader)).thenReturn(validJson);

        JSONObject result = jsonReader.getActiveUserInfo(userId, authHeader);

        assertNotNull(result);
        assertEquals(Boolean.TRUE, result.get("success"));
        assertEquals("User found", result.get("message"));
    }

    @Test
    void getActiveUserInfo_invalidJson_returnsEmptyJsonObject() {
        when(adminAPICall.validateActiveUser("u", "Bearer t")).thenReturn("invalid-json");

        JSONObject result = jsonReader.getActiveUserInfo("u", "Bearer t");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

  

    @Test
    void getMessageFromResponse_readsField() {
        JSONObject o = new JSONObject();
        o.put("message", "ok");
        assertEquals("ok", jsonReader.getMessageFromResponse(o));
    }

    @Test
    void getSuccessFromResponse_readsField() {
        JSONObject o = new JSONObject();
        o.put("success", true);
        assertEquals(Boolean.TRUE, jsonReader.getSuccessFromResponse(o));
    }

   

    @Test
    void getUserObject_extractsUserFields() {
        JSONObject data = new JSONObject();
        data.put("username", "alice");
        data.put("email", "alice@example.com");
        data.put("role", "USER");
        data.put("userID", "456");

        JSONObject wrapper = new JSONObject();
        wrapper.put("data", data);

        User u = jsonReader.getUserObject(wrapper);
        assertEquals("alice", u.getUsername());
        assertEquals("alice@example.com", u.getEmail());
        assertEquals("USER", u.getRole());
        assertEquals("456", u.getUserId());
    }

    @Test
    void getDataFromResponse_nullOrEmpty_returnsNull() {
        assertNull(jsonReader.getDataFromResponse(null));
        JSONObject empty = new JSONObject();
        assertNull(jsonReader.getDataFromResponse(empty));
    }


    @Test
    void getValidationRules_whenApiReturnsBody_usesMapper_andReturnsPolicyRoot() {
        when(policyAPICall.getRuleByPolicyId("P1", "Bearer t")).thenReturn("{\"some\":\"json\"}");
        PolicyRoot pr = new PolicyRoot();
        when(mapper.getPolicyAndRules("{\"some\":\"json\"}")).thenReturn(pr);

        PolicyRoot out = jsonReader.getValidationRules("P1", "Bearer t");
        assertSame(pr, out);
    }

    @Test
    void getValidationRules_whenApiReturnsEmpty_returnsNull() {
        when(policyAPICall.getRuleByPolicyId("P2", "Bearer t")).thenReturn("");
        assertNull(jsonReader.getValidationRules("P2", "Bearer t"));
    }

    @Test
    void getValidationRules_whenMapperThrows_returnsNull() {
        when(policyAPICall.getRuleByPolicyId("P3", "Bearer t")).thenReturn("{bad}");
        when(mapper.getPolicyAndRules("{bad}")).thenThrow(new RuntimeException("boom"));

        PolicyRoot out = jsonReader.getValidationRules("P3", "Bearer t");
        assertNull(out);
    }


    @Test
    void getAccessToken_validJson_returnsToken() {
        String json = "{\"data\":{\"token\":\"abc123\"}}";
        when(adminAPICall.getAccessToken("user@example.com")).thenReturn(json);

        String tok = jsonReader.getAccessToken("user@example.com");
        assertEquals("abc123", tok);
    }

    @Test
    void getAccessToken_invalidJson_returnsEmptyString() {
        when(adminAPICall.getAccessToken("x")).thenReturn("not-json");
        assertEquals("", jsonReader.getAccessToken("x"));
    }

    @Test
    void getOrganizationName_validJson_extractsName() {
        String json = "{\"data\":{\"organizationName\":\"Acme Corp\"}}";
        when(orgAPICall.validateActiveOrganization("ORG-1", "Bearer t")).thenReturn(json);

        String name = jsonReader.getOrganizationName("ORG-1", "Bearer t");
        assertEquals("Acme Corp", name);
    }

    @Test
    void getOrganizationName_invalidJson_returnsEmptyString() {
        when(orgAPICall.validateActiveOrganization("ORG-2", "Bearer t")).thenReturn("invalid");
        assertEquals("", jsonReader.getOrganizationName("ORG-2", "Bearer t"));
    }
}
