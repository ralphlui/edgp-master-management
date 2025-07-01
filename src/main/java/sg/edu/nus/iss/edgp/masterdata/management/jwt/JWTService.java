package sg.edu.nus.iss.edgp.masterdata.management.jwt;

import java.util.function.Function;

import org.json.simple.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;


import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import lombok.RequiredArgsConstructor; 
import sg.edu.nus.iss.edgp.masterdata.management.configuration.JWTConfig;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.User;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Date;

@Service
@RequiredArgsConstructor
public class JWTService {
	
	private final ApplicationContext context;
	
	private final JWTConfig jwtConfig;
	private final JSONReader jsonReader; 
	public static final String ORG_ID = "orgId";
	public static final String SCOPE = "scope";
	
	public static final String USER_EMAIL = "userEmail";
	public static final String CLAIM_USERNAME = "userName";

	public UserDetails getUserDetail(String authorizationHeader, String token)
			throws JwtException, IllegalArgumentException, Exception {
		String userID = extractSubject(token);

		JSONObject userJSONObjet = jsonReader.getActiveUserInfo(userID, authorizationHeader);
		Boolean success = jsonReader.getSuccessFromResponse(userJSONObjet);
	
		if (success) {
			User user = jsonReader.getUserObject(userJSONObjet);
			return org.springframework.security.core.userdetails.User.withUsername(user.getEmail())
					.password(user.getPassword()).roles(user.getRole().toString()).build();
		} else {
			String message = jsonReader.getMessageFromResponse(userJSONObjet);
			throw new Exception(message);
		}

	}
	
	public PublicKey loadPublicKey() throws Exception {
		byte[] keyBytes = Base64.getDecoder().decode(jwtConfig.getJWTPubliceKey());
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		return keyFactory.generatePublic(new X509EncodedKeySpec(keyBytes));
	}

	
	public String extractSubject(String token) throws JwtException, IllegalArgumentException, Exception {
		// TODO Auto-generated method stub
		return extractClaim(token, Claims::getSubject);
	}
	
	public <T> T extractClaim(String token, Function<Claims, T> claimResolver)
			throws JwtException, IllegalArgumentException, Exception {
		final Claims cliams = extractAllClaims(token);
		return claimResolver.apply(cliams);
	}
	
	
	public Claims extractAllClaims(String token) throws JwtException, IllegalArgumentException, Exception {
		return Jwts.parser().verifyWith(this.loadPublicKey()).build().parseSignedClaims(token).getPayload();
	}
	
	
	public Boolean validateToken(String token, UserDetails userDetails)
			throws JwtException, IllegalArgumentException, Exception {
		Claims claims = extractAllClaims(token);
		String userEmail = claims.get(USER_EMAIL, String.class);
		return (userEmail.equals(userDetails.getUsername()) && !isTokenExpired(token));
	}
	
	public boolean isTokenExpired(String token) throws JwtException, IllegalArgumentException, Exception {
		return extractExpiration(token).before(new Date());
	}

	
	public Date extractExpiration(String token) throws JwtException, IllegalArgumentException, Exception {
		return extractClaim(token, Claims::getExpiration);
	}
	
	public String extractUserNameFromToken(String token) {
	    try {
	        Claims claims = extractAllClaims(token);
	        return claims.get(CLAIM_USERNAME, String.class);
	    } catch (ExpiredJwtException e) {
	        return e.getClaims().get(CLAIM_USERNAME, String.class);
	    } catch (Exception e) {
	        return "Invalid Username";
	    }
	}
	
	
	public String extractUserIdFromToken(String token) {
		try {
			Claims claims = extractAllClaims(token);
			return claims.getSubject();
		} catch (ExpiredJwtException e) {
			return e.getClaims().getSubject();
		} catch (Exception e) {
			return "Invalid UserID";
		}
	}
	
	public String extractOrgIdFromToken(String token) {
		try {
			Claims claims = extractAllClaims(token);
			return claims.get(ORG_ID, String.class);
		}  catch (Exception e) {
			return "Invalid OrgId";
		}
	}
	
	public String extractScopeFromToken(String token) {
		try {
			Claims claims = extractAllClaims(token);
			return claims.get(SCOPE, String.class);
		}  catch (Exception e) {
			return "Invalid Scope";
		}
	}
	
	public String extractUserIdAllowExpiredToken(String token) throws JwtException, IllegalArgumentException, Exception {
		try {
			return extractClaim(token, Claims::getSubject);
		} catch (ExpiredJwtException e) {
			return e.getClaims().getSubject();
		} catch (Exception e) {
			return AuditLogInvalidUser.INVALID_USER_ID.toString();
		}
	}
	
	public String retrieveUserName(String token) throws JwtException, IllegalArgumentException, Exception {
		try {
			Claims claims = extractAllClaims(token);
			String userName = claims.get("userName", String.class);
			return userName;
		} catch (ExpiredJwtException e) {
			return e.getClaims().get("userName", String.class);
		} catch (Exception e) {
			return "Invalid Username";
		}
	}
	
	public String extractUserNameAllowExpiredToken(String token) throws JwtException, IllegalArgumentException, Exception {
		try {
			Claims claims = extractAllClaims(token);
			String userName = claims.get(CLAIM_USERNAME, String.class);
			return userName;
		} catch (ExpiredJwtException e) {
			return e.getClaims().get(CLAIM_USERNAME, String.class);
		} catch (Exception e) {
			return AuditLogInvalidUser.INVALID_USER_NAME.toString();
		}
	}


}
