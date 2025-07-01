package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.http.HttpServletResponse;

public class JwtTokenErrorResponse {

	public static void sendErrorResponse(HttpServletResponse response, String message, int status)
			throws JsonProcessingException, IOException {
		response.setStatus(status);
		response.setContentType("application/json");

		Map<String, Object> errorDetails = new HashMap<>();
		ObjectMapper objectMapper = new ObjectMapper();

		errorDetails.put("success", false);
		errorDetails.put("message", message);
		errorDetails.put("totalRecord", 0);
		errorDetails.put("data", null);
		errorDetails.put("status", status);

		response.getWriter().write(objectMapper.writeValueAsString(errorDetails));
	}
}
