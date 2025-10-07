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
class OrganizationAPICallTest {

    private OrganizationAPICall svc;

    @BeforeEach
    void setUp() {
        svc = new OrganizationAPICall();
        
        ReflectionTestUtils.setField(svc, "orgURL", "  https://api.example.com  ");
    }

    @Test
    void validateActiveOrganization_success_buildsRequestCorrectly_andReturnsBody() throws Exception {
        String orgId = "ORG-123";
        String auth = "Bearer abc";
        String expectedUrl = "https://api.example.com/my-organization";

         
        HttpClient.Builder builder = mock(HttpClient.Builder.class);
        HttpClient client = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = (HttpResponse<String>) mock(HttpResponse.class);

        
        try (MockedStatic<HttpClient> httpClientStatic = mockStatic(HttpClient.class)) {
            httpClientStatic.when(HttpClient::newBuilder).thenReturn(builder);
            when(builder.connectTimeout(Duration.ofSeconds(30))).thenReturn(builder);
            when(builder.build()).thenReturn(client);

            when(response.body()).thenReturn("{\"ok\":true}");

            @SuppressWarnings("unchecked")
            ArgumentCaptor<HttpRequest> reqCap = ArgumentCaptor.forClass(HttpRequest.class);

             
            when(client.send(reqCap.capture(), any(HttpResponse.BodyHandler.class)))
                    .thenReturn(response);

             
            String body = svc.validateActiveOrganization(orgId, auth);

            
            assertEquals("{\"ok\":true}", body);

             
            HttpRequest sent = reqCap.getValue();
            assertEquals(expectedUrl, sent.uri().toString());
            assertEquals("GET", sent.method());
            assertTrue(sent.timeout().isPresent());
            assertEquals(Duration.ofSeconds(30), sent.timeout().get());

            assertEquals(auth, sent.headers().firstValue("Authorization").orElse(null));
            assertEquals(orgId, sent.headers().firstValue("X-Org-Id").orElse(null));
            assertEquals("application/json", sent.headers().firstValue("Content-Type").orElse(null));
        }
    }

    @Test
    void validateActiveOrganization_whenClientThrows_returnsEmptyString() throws Exception {
        
        ReflectionTestUtils.setField(svc, "orgURL", "http://localhost");

        HttpClient.Builder builder = mock(HttpClient.Builder.class);
        HttpClient client = mock(HttpClient.class);

        try (MockedStatic<HttpClient> httpClientStatic = mockStatic(HttpClient.class)) {
            httpClientStatic.when(HttpClient::newBuilder).thenReturn(builder);
            when(builder.connectTimeout(Duration.ofSeconds(30))).thenReturn(builder);
            when(builder.build()).thenReturn(client);

           
            when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                    .thenThrow(new java.io.IOException("boom"));

            String body = svc.validateActiveOrganization("ORG-1", "Bearer t");
            assertEquals("", body);
        }
    }
}
