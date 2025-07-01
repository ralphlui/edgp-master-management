package sg.edu.nus.iss.edgp.masterdata.management.jwt;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.MalformedJwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.masterdata.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService; 

@Component
@RequiredArgsConstructor
public class JWTFilter extends OncePerRequestFilter {
	
	@Value("${audit.activity.type.prefix}")
	String activityTypePrefix;

	private final JWTService jwtService;
	private final AuditService auditLogService;
	 

	private String userID;
	private String userName;
	private String apiEndpoint;
	private HTTPVerb httpMethod;
 
	
	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
			throws ServletException, IOException {

		String authHeader = request.getHeader("Authorization");
	    apiEndpoint = request.getRequestURI();
	    String methodName = request.getMethod();
	    httpMethod = HTTPVerb.valueOf(methodName);
	    
	    userID = AuditLogInvalidUser.INVALID_USER_ID.toString();
	    userName = AuditLogInvalidUser.INVALID_USER_NAME.toString();
	    String requestURI = request.getRequestURI(); 

		 
		
		if (authHeader == null || !authHeader.startsWith("Bearer ")) {
			handleException(response, "Authorization header is missing or invalid.",
					HttpServletResponse.SC_UNAUTHORIZED, "");
			return;
		}

		String jwtToken = authHeader.substring(7); // Remove "Bearer " prefix
	

		if (SecurityContextHolder.getContext().getAuthentication() == null && !jwtToken.isEmpty()) {
			try {
		    userID = jwtService.extractUserIdAllowExpiredToken(jwtToken);	
		    userName = jwtService.extractUserNameAllowExpiredToken(jwtToken);
		    UserDetails userDetails = jwtService.getUserDetail(authHeader, jwtToken);
				if (jwtService.validateToken(jwtToken, userDetails)) {
					UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(
							userDetails, null, userDetails.getAuthorities());
					authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

					SecurityContextHolder.getContext().setAuthentication(authentication);
				}
			} catch (ExpiredJwtException e) {
				handleException(response, "JWT token is expired", HttpServletResponse.SC_UNAUTHORIZED,jwtToken);
				return;
			} catch (MalformedJwtException e) {
				handleException(response, "Invalid JWT token", HttpServletResponse.SC_UNAUTHORIZED,jwtToken);
				return;
			} catch (SecurityException e) {
				handleException(response, "JWT signature is invalid", HttpServletResponse.SC_UNAUTHORIZED,jwtToken);
				return;
			} catch (Exception e) {
				handleException(response, e.getMessage(), HttpServletResponse.SC_UNAUTHORIZED,jwtToken);
				return;
			}
			
			
		}

		filterChain.doFilter(request, response);
	}

	private void handleException(HttpServletResponse response, String message, int status, String token)
			throws IOException {
		String activityType = "Authentication-JWTValidation";
		TokenErrorResponse.sendErrorResponse(response, message, status, "UnAuthorized");
		AuditDTO auditDTO = auditLogService.createAuditDTO(userID,activityType, activityTypePrefix, apiEndpoint, httpMethod);
		auditLogService.logAudit(auditDTO, status, message, token);
	}
}