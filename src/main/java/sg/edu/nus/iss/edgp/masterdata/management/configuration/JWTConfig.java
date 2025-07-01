package sg.edu.nus.iss.edgp.masterdata.management.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JWTConfig {

	@Value("${jwt.public.key}")
	private String jwtPublicKey;
	
	@Bean
	public String getJWTPubliceKey() {
		return jwtPublicKey.replaceAll("\\s", "");
	}

}