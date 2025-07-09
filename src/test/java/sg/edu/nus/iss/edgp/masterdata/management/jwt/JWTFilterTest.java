package sg.edu.nus.iss.edgp.masterdata.management.jwt;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.userdetails.UserDetails;

import io.jsonwebtoken.JwtException;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;

import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;

public class JWTFilterTest {

    @Mock
    private JWTService jwtService;

    @Mock
    private AuditService auditService;

    @Mock
    private FilterChain filterChain;

    @Mock
    private UserDetails userDetails;

    @InjectMocks
    private JWTFilter jwtFilter;

    private MockHttpServletRequest request;
    private MockHttpServletResponse response;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
    }

    @Test
    public void testDoFilterInternal_ValidToken() throws JwtException, IllegalArgumentException, Exception {
        String token = "valid.token.here";
        request.addHeader("Authorization", "Bearer " + token);
        request.setRequestURI("/api/test");
        request.setMethod("GET");

        when(jwtService.extractUserIdAllowExpiredToken(token)).thenReturn("123");
        when(jwtService.extractUserNameAllowExpiredToken(token)).thenReturn("testuser");
        when(jwtService.getUserDetail(anyString(), eq(token))).thenReturn(userDetails);
        when(jwtService.validateToken(eq(token), eq(userDetails))).thenReturn(true);
        when(userDetails.getAuthorities()).thenReturn(null);

        jwtFilter.doFilterInternal(request, response, filterChain);

        assertNotNull(SecurityContextHolder.getContext().getAuthentication());
        verify(filterChain, times(1)).doFilter(request, response);
    }

    @Test
    public void testDoFilterInternal_InvalidHeader() throws ServletException, IOException {
        request.setRequestURI("/api/test");
        request.setMethod("GET");
        request.addHeader("Authorization", "InvalidToken");

        jwtFilter.doFilterInternal(request, response, filterChain);

        assertEquals(401, response.getStatus());
    }
}
