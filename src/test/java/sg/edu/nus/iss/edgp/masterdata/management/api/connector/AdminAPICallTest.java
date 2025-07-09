package sg.edu.nus.iss.edgp.masterdata.management.api.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class AdminAPICallTest {
	
	private AdminAPICall adminAPICall;

	@Mock
	private HttpClient httpClient;

	@Mock
	private HttpResponse<String> httpResponse;

	@Mock
	private HttpClient httpClientMock;

	@Mock
	private HttpResponse<String> httpResponseMock;

	@BeforeEach
	void setUp() {
		MockitoAnnotations.openMocks(this);
		adminAPICall = new AdminAPICall();

		try {
			java.lang.reflect.Field field = AdminAPICall.class.getDeclaredField("adminURL");
			field.setAccessible(true);
			field.set(adminAPICall, "http://fake-auth-url.com/");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void testValidateActiveUser_ExceptionHandling() throws Exception {
		when(httpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
				.thenThrow(new RuntimeException("Connection error"));

		String result = adminAPICall.validateActiveUser("user123", "Bearer xyz");

		assertEquals("", result);
	}

	@Test
	void testHttpClientSendReturnsExpectedResponse() throws Exception {
		String expectedResponseBody = "{\"status\":\"active\"}";

		when(httpResponseMock.body()).thenReturn(expectedResponseBody);

		when(httpClientMock.send(any(HttpRequest.class), Mockito.<HttpResponse.BodyHandler<String>>any()))
				.thenReturn(httpResponseMock);

		HttpRequest request = HttpRequest.newBuilder().uri(new java.net.URI("http://example.com")).GET().build();

		HttpResponse<String> response = httpClientMock.send(request, HttpResponse.BodyHandlers.ofString());
		String actualBody = response.body();

		assertEquals(expectedResponseBody, actualBody);
	}

}
