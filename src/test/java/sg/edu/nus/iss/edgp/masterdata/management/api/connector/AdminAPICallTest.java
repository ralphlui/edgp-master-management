package sg.edu.nus.iss.edgp.masterdata.management.api.connector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class AdminAPICallTest {

    private AdminAPICall svc;

    @BeforeEach
    void setUp() {
        svc = new AdminAPICall();
       
        ReflectionTestUtils.setField(svc, "adminURL", "   https://api.example.com   ");
    }

    

    @Test
    void validateActiveUser_success_buildsRequestCorrectly_andReturnsBody() throws Exception {
        String userId = "U-123";
        String auth = "Bearer abc";
        String expectedUrl = "https://api.example.com/users/profile";

        HttpClient.Builder builder = mock(HttpClient.Builder.class);
        HttpClient client = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = (HttpResponse<String>) mock(HttpResponse.class);

        try (MockedStatic<HttpClient> http = mockStatic(HttpClient.class)) {
            http.when(HttpClient::newBuilder).thenReturn(builder);
            when(builder.connectTimeout(Duration.ofSeconds(30))).thenReturn(builder);
            when(builder.build()).thenReturn(client);

            when(response.body()).thenReturn("{\"ok\":true}");

            @SuppressWarnings("unchecked")
            ArgumentCaptor<HttpRequest> reqCap = ArgumentCaptor.forClass(HttpRequest.class);
            when(client.send(reqCap.capture(), any(HttpResponse.BodyHandler.class)))
                    .thenReturn(response);

            String body = svc.validateActiveUser(userId, auth);

            assertEquals("{\"ok\":true}", body);

            HttpRequest sent = reqCap.getValue();
            assertEquals(expectedUrl, sent.uri().toString());
            assertEquals("GET", sent.method());
            assertTrue(sent.timeout().isPresent());
            assertEquals(Duration.ofSeconds(30), sent.timeout().get());

            assertEquals(auth, sent.headers().firstValue("Authorization").orElse(null));
            assertEquals(userId, sent.headers().firstValue("X-User-Id").orElse(null));
            assertEquals("application/json", sent.headers().firstValue("Content-Type").orElse(null));
        }
    }

    @Test
    void validateActiveUser_whenClientThrows_returnsEmptyString() throws Exception {
        ReflectionTestUtils.setField(svc, "adminURL", "http://localhost");

        HttpClient.Builder builder = mock(HttpClient.Builder.class);
        HttpClient client = mock(HttpClient.class);

        try (MockedStatic<HttpClient> http = mockStatic(HttpClient.class)) {
            http.when(HttpClient::newBuilder).thenReturn(builder);
            when(builder.connectTimeout(Duration.ofSeconds(30))).thenReturn(builder);
            when(builder.build()).thenReturn(client);

            when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                    .thenThrow(new java.io.IOException("boom"));

            String body = svc.validateActiveUser("U-1", "Bearer t");
            assertEquals("", body);
        }
    }

    // -------- getAccessToken --------

    @Test
    void getAccessToken_success_buildsRequestCorrectly_andReturnsBody() throws Exception {
        String email = "user@example.com";
        String expectedUrl = "https://api.example.com/users/accessToken";

        HttpClient.Builder builder = mock(HttpClient.Builder.class);
        HttpClient client = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = (HttpResponse<String>) mock(HttpResponse.class);

        try (MockedStatic<HttpClient> http = mockStatic(HttpClient.class)) {
            http.when(HttpClient::newBuilder).thenReturn(builder);
            when(builder.connectTimeout(Duration.ofSeconds(30))).thenReturn(builder);
            when(builder.build()).thenReturn(client);

            when(response.body()).thenReturn("{\"token\":\"abc\"}");

            @SuppressWarnings("unchecked")
            ArgumentCaptor<HttpRequest> reqCap = ArgumentCaptor.forClass(HttpRequest.class);
            when(client.send(reqCap.capture(), any(HttpResponse.BodyHandler.class)))
                    .thenReturn(response);

            String body = svc.getAccessToken(email);

            assertEquals("{\"token\":\"abc\"}", body);

            HttpRequest sent = reqCap.getValue();
            assertEquals(expectedUrl, sent.uri().toString());
            assertEquals("GET", sent.method());
            assertTrue(sent.timeout().isPresent());
            assertEquals(Duration.ofSeconds(30), sent.timeout().get());

            assertEquals(email, sent.headers().firstValue("X-User-Email").orElse(null));
            assertEquals("application/json", sent.headers().firstValue("Content-Type").orElse(null));
        }
    }

    @Test
    void getAccessToken_whenClientThrows_returnsEmptyString() throws Exception {
        ReflectionTestUtils.setField(svc, "adminURL", "http://localhost");

        HttpClient.Builder builder = mock(HttpClient.Builder.class);
        HttpClient client = mock(HttpClient.class);

        try (MockedStatic<HttpClient> http = mockStatic(HttpClient.class)) {
            http.when(HttpClient::newBuilder).thenReturn(builder);
            when(builder.connectTimeout(Duration.ofSeconds(30))).thenReturn(builder);
            when(builder.build()).thenReturn(client);

            when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                    .thenThrow(new java.io.IOException("boom"));

            String body = svc.getAccessToken("user@example.com");
            assertEquals("", body);
        }
    }
}
