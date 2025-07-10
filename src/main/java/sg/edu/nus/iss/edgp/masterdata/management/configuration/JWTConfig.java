package sg.edu.nus.iss.edgp.masterdata.management.configuration;

import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

@Configuration
public class JWTConfig {

	@Value("${jwt.public.key}")
	private String jwtPublicKey;
	
	@Bean
	public String getJWTPubliceKey() {
		return jwtPublicKey.replaceAll("\\s", "");
	}
	
	@Bean
	@Profile("!test")
	public JwtDecoder jwtDecoder(RSAPublicKey publicKey) {
		return NimbusJwtDecoder.withPublicKey(publicKey).build();
	}
	
	@Bean
	@Profile("!test") 
	public RSAPublicKey loadPublicKey() throws Exception {
		
		byte[] decoded = Base64.getDecoder().decode(getJWTPubliceKey());
		X509EncodedKeySpec keySpec = new X509EncodedKeySpec(decoded);
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		return (RSAPublicKey) keyFactory.generatePublic(keySpec);
	}

}