package sg.edu.nus.iss.edgp.masterdata.management.utility;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;

class JSONDataMapperTest {

    private JSONDataMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new JSONDataMapper();
    }

    @Test
    void getPolicyAndRules_emptyString_returnsDefaultInstance() {
        PolicyRoot root = mapper.getPolicyAndRules("");
        assertNotNull(root); 
    }

    @Test
    void getPolicyAndRules_validMinimalJson_returnsInstance() {
        String json = "{}";
        PolicyRoot root = mapper.getPolicyAndRules(json);
        assertNotNull(root);
    }

    @Test
    void getPolicyAndRules_invalidJson_throwsRuntimeException() {
        
        assertThrows(RuntimeException.class, () -> mapper.getPolicyAndRules("   "));
        assertThrows(RuntimeException.class, () -> mapper.getPolicyAndRules("{not-json"));
    }
}

