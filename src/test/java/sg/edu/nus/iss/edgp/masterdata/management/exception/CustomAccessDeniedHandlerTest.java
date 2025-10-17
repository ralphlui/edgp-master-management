package sg.edu.nus.iss.edgp.masterdata.management.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.access.AccessDeniedException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class CustomAccessDeniedHandlerTest {

    @Test
    void handle_sets403AndWritesJson_usingSpringMockResponse() throws Exception {
       
        CustomAccessDeniedHandler handler = new CustomAccessDeniedHandler();
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        AccessDeniedException ex = new AccessDeniedException("forbidden");

        handler.handle(request, response, ex);

        assertThat(response.getStatus()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(response.getContentType()).isEqualTo("application/json");

        String body = response.getContentAsString();
        assertThat(body).isNotBlank();
        assertThat(body)
                .contains("You do not have the required scope to perform this action.");

        Map<?, ?> parsed = new ObjectMapper().readValue(body, Map.class);
        assertThat(parsed).isNotNull();
    }

    @Test
    void handle_invokesResponseMethods_andWritesExpectedPayload_usingMockito() throws Exception {
     
        CustomAccessDeniedHandler handler = new CustomAccessDeniedHandler();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        AccessDeniedException ex = new AccessDeniedException("forbidden");

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        when(response.getWriter()).thenReturn(pw);

        handler.handle(request, response, ex);

        verify(response).setStatus(HttpStatus.FORBIDDEN.value());
        verify(response).setContentType("application/json");
        verify(response).getWriter();

        
        pw.flush();
        String body = sw.toString();
        assertThat(body)
                .contains("You do not have the required scope to perform this action.");
    }
}
