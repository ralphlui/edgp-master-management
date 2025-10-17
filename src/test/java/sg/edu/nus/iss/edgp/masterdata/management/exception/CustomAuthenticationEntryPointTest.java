package sg.edu.nus.iss.edgp.masterdata.management.exception;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.AuthenticationException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class CustomAuthenticationEntryPointTest {


    static class StubAuthException extends AuthenticationException {
        public StubAuthException(String msg) { super(msg); }
    }

    @Test
    void commence_setsStatusAndWritesJson_usingSpringMockResponse() throws Exception {
      
        CustomAuthenticationEntryPoint entryPoint = new CustomAuthenticationEntryPoint();
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        AuthenticationException ex = new StubAuthException("Bad or missing token");

        entryPoint.commence(request, response, ex);

        assertThat(response.getStatus()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(response.getContentType()).isEqualTo("application/json");

        String body = response.getContentAsString();
        assertThat(body).isNotBlank();

        assertThat(body).contains("Invalid or missing token.");

        Map<?, ?> parsed = new ObjectMapper().readValue(body, Map.class);
        assertThat(parsed).isNotNull();
    }

    @Test
    void commence_invokesResponseMethods_usingMockito() throws Exception {
       
        CustomAuthenticationEntryPoint entryPoint = new CustomAuthenticationEntryPoint();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        AuthenticationException ex = new StubAuthException("No auth");

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        when(response.getWriter()).thenReturn(pw);

       
        entryPoint.commence(request, response, ex);

        
        verify(response).setStatus(HttpStatus.UNAUTHORIZED.value());
        verify(response).setContentType("application/json");
        verify(response).getWriter();

        pw.flush();
        String body = sw.toString();
        assertThat(body).contains("Invalid or missing token.");
    }
}
