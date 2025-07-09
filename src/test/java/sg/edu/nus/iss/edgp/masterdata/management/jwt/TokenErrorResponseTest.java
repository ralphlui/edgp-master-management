package sg.edu.nus.iss.edgp.masterdata.management.jwt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
 

public class TokenErrorResponseTest {
	@Test
    void testSendErrorResponse() throws IOException {
        // Given
        MockHttpServletResponse response = new MockHttpServletResponse();
        String message = "Invalid token";
        int status = 401;
        String error = "Unauthorized"; 

        // When
        TokenErrorResponse.sendErrorResponse(response, message, status, error);

        // Then
        assertEquals(status, response.getStatus());
        assertEquals("application/json", response.getContentType());

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> responseMap = mapper.readValue(response.getContentAsString(), Map.class);

        assertEquals(false, responseMap.get("success"));
        assertEquals(message, responseMap.get("message"));
        assertEquals(0, responseMap.get("totalRecord"));
        assertNull(responseMap.get("data"));
        assertEquals(status, responseMap.get("status"));
    }
}
