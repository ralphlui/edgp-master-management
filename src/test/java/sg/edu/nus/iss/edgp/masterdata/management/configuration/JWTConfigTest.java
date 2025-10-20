package sg.edu.nus.iss.edgp.masterdata.management.configuration;

import static org.junit.jupiter.api.Assertions.*;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.util.ReflectionTestUtils;

public class JWTConfigTest {

   
    private static String generateB64RsaPublicKey() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair kp = kpg.generateKeyPair();
        return Base64.getEncoder().encodeToString(kp.getPublic().getEncoded());
    }

    @Test
    void getJWTPubliceKey_stripsWhitespace() {
        JWTConfig cfg = new JWTConfig();
        // inject property value
        ReflectionTestUtils.setField(cfg, "jwtPublicKey", "  ABC \n DEF \t 123  ");
        String out = cfg.getJWTPubliceKey();
        assertEquals("ABCDEF123", out);
    }

    @Test
    void beansPresent_whenProfileNotTest() throws Exception {
        String b64 = generateB64RsaPublicKey();

        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withInitializer(ctx ->
                        ((ConfigurableEnvironment) ctx.getEnvironment()).setActiveProfiles("dev")) // not "test"
                .withPropertyValues("jwt.public.key=" + b64)
                .withUserConfiguration(JWTConfig.class);

        contextRunner.run(ctx -> {
            assertTrue(ctx.containsBean("getJWTPubliceKey"));
            assertEquals(String.class, ctx.getBean("getJWTPubliceKey").getClass());

            // @Profile("!test") beans should be created
            RSAPublicKey publicKey = ctx.getBean(RSAPublicKey.class);
            assertNotNull(publicKey);
            assertNotNull(publicKey.getModulus());
            assertNotNull(publicKey.getPublicExponent());

            JwtDecoder decoder = ctx.getBean(JwtDecoder.class);
            assertNotNull(decoder);
        });
    }

    @Test
    void beansExcluded_whenProfileIsTest() throws Exception {
        String b64 = generateB64RsaPublicKey();

        ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withInitializer(ctx ->
                        ((ConfigurableEnvironment) ctx.getEnvironment()).setActiveProfiles("test")) // triggers @Profile("!test") exclusion
                .withPropertyValues("jwt.public.key=" + b64)
                .withUserConfiguration(JWTConfig.class);

        contextRunner.run(ctx -> {
            // String bean is not profiled -> present
            assertTrue(ctx.containsBean("getJWTPubliceKey"));
            assertEquals(String.class, ctx.getBean("getJWTPubliceKey").getClass());

            // RSAPublicKey and JwtDecoder are @Profile("!test") -> not present
            assertFalse(ctx.getBeansOfType(RSAPublicKey.class).size() > 0);
            assertFalse(ctx.getBeansOfType(JwtDecoder.class).size() > 0);
        });
    }
}
