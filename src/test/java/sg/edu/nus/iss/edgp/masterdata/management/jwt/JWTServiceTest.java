package sg.edu.nus.iss.edgp.masterdata.management.jwt;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.security.interfaces.RSAPublicKey;
import java.util.Date;

import org.json.simple.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.Jwts;
import sg.edu.nus.iss.edgp.masterdata.management.configuration.JWTConfig;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;

@ExtendWith(MockitoExtension.class)
public class JWTServiceTest {

    private ApplicationContext applicationContext;
    private JWTConfig jwtConfig;
    private JSONReader jsonReader;

    private JWTService svc;

    @BeforeEach
    void setUp() {
        applicationContext = mock(ApplicationContext.class);
        jwtConfig = mock(JWTConfig.class);
        jsonReader = mock(JSONReader.class);
        svc = new JWTService(applicationContext, jwtConfig, jsonReader);
    }
 

    /** Stubs JJWT to return provided Claims for the given token. */
    private MockedStatic<Jwts> stubJwtClaims(String token, Claims claims) throws Exception {
        MockedStatic<Jwts> jwts = mockStatic(Jwts.class);
        JwtParserBuilder builder = mock(JwtParserBuilder.class);
        JwtParser parser = mock(JwtParser.class);
        @SuppressWarnings("unchecked")
        Jws<Claims> jws = (Jws<Claims>) mock(Jws.class);
        RSAPublicKey pk = mock(RSAPublicKey.class);

        when(jwtConfig.loadPublicKey()).thenReturn(pk);
        jwts.when(Jwts::parser).thenReturn(builder);
        when(builder.verifyWith(pk)).thenReturn(builder);
        when(builder.build()).thenReturn(parser);
        when(parser.parseSignedClaims(token)).thenReturn(jws);
        when(jws.getPayload()).thenReturn(claims);
        return jwts;
    }
 
    private MockedStatic<Jwts> stubJwtThrows(String token, RuntimeException toThrow) throws Exception {
        MockedStatic<Jwts> jwts = mockStatic(Jwts.class);
        JwtParserBuilder builder = mock(JwtParserBuilder.class);
        JwtParser parser = mock(JwtParser.class);
        RSAPublicKey pk = mock(RSAPublicKey.class);

        when(jwtConfig.loadPublicKey()).thenReturn(pk);
        jwts.when(Jwts::parser).thenReturn(builder);
        when(builder.verifyWith(pk)).thenReturn(builder);
        when(builder.build()).thenReturn(parser);
        when(parser.parseSignedClaims(token)).thenThrow(toThrow);
        return jwts;
    }
 

    @Test
    void extractUserEmailFromToken_happy() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.get("userEmail", String.class)).thenReturn("e@x.com");

        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("e@x.com", svc.extractUserEmailFromToken(token));
        }
    }

    @Test
    void extractUserEmailFromToken_expired_usesClaimsFromException() throws Exception {
        String token = "tok";
        Claims expClaims = mock(Claims.class);
        when(expClaims.get("userEmail", String.class)).thenReturn("late@x.com");
        ExpiredJwtException ex = mock(ExpiredJwtException.class);
        when(ex.getClaims()).thenReturn(expClaims);

        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, ex)) {
            assertEquals("late@x.com", svc.extractUserEmailFromToken(token));
        }
    }

    @Test
    void extractUserNameFromToken_invalid_returnsDefault() throws Exception {
        String token = "tok";
        
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, new RuntimeException("parse fail"))) {
            assertEquals("Invalid Username", svc.extractUserNameFromToken(token));
        }
    }

    @Test
    void extractUserIdFromToken_happy_and_expired_and_invalid() throws Exception {
        String token = "tok";

       
        Claims claims = mock(Claims.class);
        when(claims.getSubject()).thenReturn("U123");
        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("U123", svc.extractUserIdFromToken(token));
        }

       
        Claims expClaims = mock(Claims.class);
        when(expClaims.getSubject()).thenReturn("U999");
        ExpiredJwtException ex = mock(ExpiredJwtException.class);
        when(ex.getClaims()).thenReturn(expClaims);
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, ex)) {
            assertEquals("U999", svc.extractUserIdFromToken(token));
        }

        
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, new RuntimeException("bad"))) {
            assertEquals("Invalid UserID", svc.extractUserIdFromToken(token));
        }
    }

    @Test
    void extractOrgIdFromToken_happy_and_invalid() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.get("orgId", String.class)).thenReturn("ORG-7");
        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("ORG-7", svc.extractOrgIdFromToken(token));
        }

        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, new RuntimeException("x"))) {
            assertEquals("Invalid OrgId", svc.extractOrgIdFromToken(token));
        }
    }

    @Test
    void extractScopeFromToken_happy_and_invalid() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.get("scope", String.class)).thenReturn("read:all");
        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("read:all", svc.extractScopeFromToken(token));
        }

        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, new RuntimeException("x"))) {
            assertEquals("Invalid Scope", svc.extractScopeFromToken(token));
        }
    }

    @Test
    void extractAPIKeyFromToken_happy_and_invalid() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.get("apiKey", String.class)).thenReturn("API-123");
        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("API-123", svc.extractAPIKeyFromToken(token));
        }

        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, new RuntimeException("x"))) {
            assertEquals("", svc.extractAPIKeyFromToken(token));
        }
    }

    

    @Test
    void validateToken_true_whenEmailMatches_andNotExpired() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.get("userEmail", String.class)).thenReturn("bob@x.com");
        when(claims.getExpiration()).thenReturn(new Date(System.currentTimeMillis() + 86_400_000L));

       
        try (MockedStatic<Jwts> jwts = mockStatic(Jwts.class)) {
            JwtParserBuilder builder = mock(JwtParserBuilder.class);
            JwtParser parser = mock(JwtParser.class);
            @SuppressWarnings("unchecked") Jws<Claims> jws = (Jws<Claims>) mock(Jws.class);
            RSAPublicKey pk = mock(RSAPublicKey.class);

            when(jwtConfig.loadPublicKey()).thenReturn(pk);
            jwts.when(Jwts::parser).thenReturn(builder);
            when(builder.verifyWith(pk)).thenReturn(builder);
            when(builder.build()).thenReturn(parser);
            when(parser.parseSignedClaims(token)).thenReturn(jws, jws);
            when(jws.getPayload()).thenReturn(claims);

            UserDetails ud = User.withUsername("bob@x.com").password("pw").roles("USER").build();
            assertTrue(svc.validateToken(token, ud));
        }
    }

    @Test
    void validateToken_false_whenExpired() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.get("userEmail", String.class)).thenReturn("ann@x.com");
        when(claims.getExpiration()).thenReturn(new Date(System.currentTimeMillis() - 1000)); // past

        try (MockedStatic<Jwts> jwts = mockStatic(Jwts.class)) {
            JwtParserBuilder builder = mock(JwtParserBuilder.class);
            JwtParser parser = mock(JwtParser.class);
            @SuppressWarnings("unchecked") Jws<Claims> jws = (Jws<Claims>) mock(Jws.class);
            RSAPublicKey pk = mock(RSAPublicKey.class);

            when(jwtConfig.loadPublicKey()).thenReturn(pk);
            jwts.when(Jwts::parser).thenReturn(builder);
            when(builder.verifyWith(pk)).thenReturn(builder);
            when(builder.build()).thenReturn(parser);
            when(parser.parseSignedClaims(token)).thenReturn(jws, jws); // twice
            when(jws.getPayload()).thenReturn(claims);

            UserDetails ud = User.withUsername("ann@x.com").password("pw").roles("USER").build();
            assertFalse(svc.validateToken(token, ud));
        }
    }

   

    @Test
    void extractUserIdAllowExpiredToken_handlesExpired_andGeneric() throws Exception {
        String token = "tok";

       
        Claims expClaims = mock(Claims.class);
        when(expClaims.getSubject()).thenReturn("U-EX");
        ExpiredJwtException ex = mock(ExpiredJwtException.class);
        when(ex.getClaims()).thenReturn(expClaims);
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, ex)) {
            assertEquals("U-EX", svc.extractUserIdAllowExpiredToken(token));
        }

         
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, new RuntimeException("x"))) {
            assertEquals(AuditLogInvalidUser.INVALID_USER_ID.toString(), svc.extractUserIdAllowExpiredToken(token));
        }
    }

    @Test
    void extractUserNameAllowExpiredToken_handlesExpired_andGeneric() throws Exception {
        String token = "tok";

        Claims expClaims = mock(Claims.class);
        when(expClaims.get("userName", String.class)).thenReturn("old-user");
        ExpiredJwtException ex = mock(ExpiredJwtException.class);
        when(ex.getClaims()).thenReturn(expClaims);
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, ex)) {
            assertEquals("old-user", svc.extractUserNameAllowExpiredToken(token));
        }

        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, new RuntimeException("x"))) {
            assertEquals(AuditLogInvalidUser.INVALID_USER_NAME.toString(), svc.extractUserNameAllowExpiredToken(token));
        }
    }

    

    @Test
    void getUserDetail_whenBackendSaysFail_throwsMessage() throws Exception {
        String auth = "Bearer abc";
        String token = "tok";

        Claims claims = mock(Claims.class);
        when(claims.getSubject()).thenReturn("U-1");

        JSONObject resp = new JSONObject();
       
        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            when(jsonReader.getActiveUserInfo("U-1", auth)).thenReturn(resp);
            when(jsonReader.getSuccessFromResponse(resp)).thenReturn(false);
            when(jsonReader.getMessageFromResponse(resp)).thenReturn("not active");

            Exception ex = assertThrows(Exception.class, () -> svc.getUserDetail(auth, token));
            assertEquals("not active", ex.getMessage());
        }
    }
    
 

    @Test
    void extractSubject_happy() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.getSubject()).thenReturn("SUB-123");

        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("SUB-123", svc.extractSubject(token));
        }
    }

    @Test
    void extractExpiration_happy() throws Exception {
        String token = "tok";
        Date exp = new Date(System.currentTimeMillis() + 60_000);
        Claims claims = mock(Claims.class);
        when(claims.getExpiration()).thenReturn(exp);

        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals(exp, svc.extractExpiration(token));
        }
    }

    @Test
    void isTokenExpired_true_whenPastDate_false_whenFutureDate() throws Exception {
        String token = "tok";
         
        Claims past = mock(Claims.class);
        when(past.getExpiration()).thenReturn(new Date(System.currentTimeMillis() - 1_000));

        
        Claims future = mock(Claims.class);
        when(future.getExpiration()).thenReturn(new Date(System.currentTimeMillis() + 86_400_000));

        try (MockedStatic<Jwts> jwts = mockStatic(Jwts.class)) {
            JwtParserBuilder builder = mock(JwtParserBuilder.class);
            JwtParser parser = mock(JwtParser.class);
            @SuppressWarnings("unchecked") Jws<Claims> jwsPast = (Jws<Claims>) mock(Jws.class);
            @SuppressWarnings("unchecked") Jws<Claims> jwsFuture = (Jws<Claims>) mock(Jws.class);
            RSAPublicKey pk = mock(RSAPublicKey.class);

            when(jwtConfig.loadPublicKey()).thenReturn(pk);
            jwts.when(Jwts::parser).thenReturn(builder);
            when(builder.verifyWith(pk)).thenReturn(builder);
            when(builder.build()).thenReturn(parser);
            // First call returns past, second call returns future
            when(parser.parseSignedClaims(token)).thenReturn(jwsPast, jwsFuture);
            when(jwsPast.getPayload()).thenReturn(past);
            when(jwsFuture.getPayload()).thenReturn(future);

            assertTrue(svc.isTokenExpired(token));   // past
            assertFalse(svc.isTokenExpired(token));  // future
        }
    }

    @Test
    void extractUserNameFromToken_happy_and_expired() throws Exception {
        String token = "tok";

         
        Claims claims = mock(Claims.class);
        when(claims.get("userName", String.class)).thenReturn("alice");
        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("alice", svc.extractUserNameFromToken(token));
        }

      
        Claims expClaims = mock(Claims.class);
        when(expClaims.get("userName", String.class)).thenReturn("old-alice");
        ExpiredJwtException ex = mock(ExpiredJwtException.class);
        when(ex.getClaims()).thenReturn(expClaims);
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, ex)) {
            assertEquals("old-alice", svc.extractUserNameFromToken(token));
        }
    }

    @Test
    void retrieveUserName_happy_and_expired() throws Exception {
        String token = "tok";

      
        Claims claims = mock(Claims.class);
        when(claims.get("userName", String.class)).thenReturn("bob");
        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            assertEquals("bob", svc.retrieveUserName(token));
        }

        
        Claims expClaims = mock(Claims.class);
        when(expClaims.get("userName", String.class)).thenReturn("old-bob");
        ExpiredJwtException ex = mock(ExpiredJwtException.class);
        when(ex.getClaims()).thenReturn(expClaims);
        try (MockedStatic<Jwts> ignored = stubJwtThrows(token, ex)) {
            assertEquals("old-bob", svc.retrieveUserName(token));
        }
    }

    @Test
    void extractClaim_withCustomResolver() throws Exception {
        String token = "tok";
        Claims claims = mock(Claims.class);
        when(claims.get("custom", String.class)).thenReturn("C-VAL");

        try (MockedStatic<Jwts> ignored = stubJwtClaims(token, claims)) {
            String out = svc.extractClaim(token, c -> c.get("custom", String.class));
            assertEquals("C-VAL", out);
        }
    }

}

