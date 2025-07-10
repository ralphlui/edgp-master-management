package sg.edu.nus.iss.edgp.masterdata.management;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@ActiveProfiles("test")
class EdgpMasterdataManagementApplicationTests {

	@Test
	void contextLoads() {
		// This test ensures that the Spring application context loads without issues.
	}

	@MockitoBean
    private JwtDecoder jwtDecoder;

}
