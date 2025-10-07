package sg.edu.nus.iss.edgp.masterdata.management.utility;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;

@ExtendWith(MockitoExtension.class)
class DataUploadValidationTest {

    private JSONReader jsonReader;
    private HeaderService headerService;
    private DynamicDetailService dynamoService;

    private DataUploadValidation validator;

    @BeforeEach
    void setUp() {
        jsonReader = mock(JSONReader.class);
        headerService = mock(HeaderService.class);
        dynamoService = mock(DynamicDetailService.class);

        validator = new DataUploadValidation(jsonReader, headerService, dynamoService);
        
        ReflectionTestUtils.setField(validator, "masterDataHeader", " master_header_tbl ");
    }

  

    @Test
    void isValidToUpload_whenFileIsNull_returnsBadRequest() {
        ValidationResult r = validator.isValidToUpload(null, mock(UploadRequest.class), "Bearer t");
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("File is required.", r.getMessage());
        verifyNoInteractions(dynamoService, headerService, jsonReader);
    }

    @Test
    void isValidToUpload_whenFileNameIsNull_returnsBadRequest() {
        MockMultipartFile file = new MockMultipartFile("file", (String) null, "text/csv", "abc".getBytes());
        ValidationResult r = validator.isValidToUpload(file, mock(UploadRequest.class), "Bearer t");
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("File is required.", r.getMessage());
        verifyNoInteractions(dynamoService, headerService, jsonReader);
    }

    @Test
    void isValidToUpload_whenTableExists_andDuplicateFilename_returnsConflict() {
        MockMultipartFile file = new MockMultipartFile("file", " data.csv ", "text/csv", "abc".getBytes());

        when(dynamoService.tableExists("master_header_tbl")).thenReturn(true);
        when(headerService.filenameExists("data.csv")).thenReturn(true);

        ValidationResult r = validator.isValidToUpload(file, mock(UploadRequest.class), "Bearer t");

        assertFalse(r.isValid());
        assertEquals(HttpStatus.CONFLICT, r.getStatus());
        assertEquals("A file named data.csv already exists. Choose a different name", r.getMessage());

        verify(dynamoService).tableExists("master_header_tbl");
        verify(headerService).filenameExists("data.csv");
        verifyNoInteractions(jsonReader);
    }

    @Test
    void isValidToUpload_whenUploadReqIsNull_returnsBadRequest() {
        MockMultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "abc".getBytes());
        when(dynamoService.tableExists("master_header_tbl")).thenReturn(false); // short-circuit filename existence

        ValidationResult r = validator.isValidToUpload(file, null, "Bearer t");
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Upload request is required.", r.getMessage());
    }

    @Test
    void isValidToUpload_whenDomainMissing_returnsBadRequest() {
        MockMultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "abc".getBytes());
        when(dynamoService.tableExists("master_header_tbl")).thenReturn(false);

        UploadRequest req = mock(UploadRequest.class);
        when(req.getDomainName()).thenReturn("");

        ValidationResult r = validator.isValidToUpload(file, req, "Bearer t");
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Domain is required.", r.getMessage());
        verify(req, atLeastOnce()).getDomainName();
        verify(req, never()).getPolicyId();
    }

    @Test
    void isValidToUpload_whenPolicyMissing_returnsBadRequest() {
        MockMultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "abc".getBytes());
        when(dynamoService.tableExists("master_header_tbl")).thenReturn(false);

        UploadRequest req = mock(UploadRequest.class);
        when(req.getDomainName()).thenReturn("customer");
        when(req.getPolicyId()).thenReturn("");

        ValidationResult r = validator.isValidToUpload(file, req, "Bearer t");
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Policy is required.", r.getMessage());
    }

    @Test
    void isValidToUpload_whenPolicyPresentAndValid_returnsOK() {
        MockMultipartFile file = new MockMultipartFile("file", "data.csv", "text/csv", "abc".getBytes());

        when(dynamoService.tableExists("master_header_tbl")).thenReturn(true);
        when(headerService.filenameExists("data.csv")).thenReturn(false);

        UploadRequest req = mock(UploadRequest.class);
        when(req.getDomainName()).thenReturn("customer");
        when(req.getPolicyId()).thenReturn("POL-1");

        when(jsonReader.getValidationRules("POL-1", "Bearer t")).thenReturn(new PolicyRoot());

        ValidationResult r = validator.isValidToUpload(file, req, "Bearer t");
        assertTrue(r.isValid());
        assertEquals(HttpStatus.OK, r.getStatus());
        assertEquals("OK", r.getMessage());
    }

    @Test
    void isValidToUpload_whenPolicyNull_returnsBadRequest() {
        MockMultipartFile file =
            new MockMultipartFile("file", "data.csv", "text/csv", "abc".getBytes());
 
        when(dynamoService.tableExists("master_header_tbl")).thenReturn(true);
        when(headerService.filenameExists("data.csv")).thenReturn(false);

        UploadRequest req = mock(UploadRequest.class);
        when(req.getDomainName()).thenReturn("customer");
        when(req.getPolicyId()).thenReturn("POL-X"); 
        when(jsonReader.getValidationRules("POL-X", "Bearer t")).thenReturn(null);

        ValidationResult r = validator.isValidToUpload(file, req, "Bearer t");

        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Policy is invalid.", r.getMessage());
    }



    @Test
    void isValidToUpsert_whenRequestNull_returnsBadRequest() {
        ValidationResult r = validator.isValidToUpsert(null, true);
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Data payload is required.", r.getMessage());
    }

    @Test
    void isValidToUpsert_whenDataEmpty_returnsBadRequest() {
        Map<String, Object> req = new HashMap<>();
        ValidationResult r = validator.isValidToUpsert(req, true);
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Data payload is required.", r.getMessage());
    }

    @Test
    void isValidToUpsert_updateWithoutId_returnsBadRequest() {
        Map<String, Object> data = new HashMap<>();
        data.put("data", Map.of("policy_id", "P", "domain_name", "D", "name", "x"));

        ValidationResult r = validator.isValidToUpsert(data, false);
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Missing or empty 'id'.", r.getMessage());
    }

    @Test
    void isValidToUpsert_missingPolicyAndDomain_listsBoth() {
        Map<String, Object> data = new HashMap<>();
        data.put("data", Map.of("id", "S1"));

        ValidationResult r = validator.isValidToUpsert(data, false);
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("Missing or empty: policy_id, domain_name", r.getMessage());
    }

    @Test
    void isValidToUpsert_updateButNoUpdatableFields_returnsBadRequest() {
        Map<String, Object> data = new HashMap<>();
        data.put("data", Map.of(
                "id", "S1",
                "policy_id", "P",
                "domain_name", "D"
        ));

        ValidationResult r = validator.isValidToUpsert(data, false);
        assertFalse(r.isValid());
        assertEquals(HttpStatus.BAD_REQUEST, r.getStatus());
        assertEquals("No updatable fields provided.", r.getMessage());
    }

    @Test
    void isValidToUpsert_createWithMinimalFields_returnsOK() {
        Map<String, Object> data = new HashMap<>();
        data.put("data", Map.of(
                "policy_id", "P",
                "domain_name", "D"
        ));

        ValidationResult r = validator.isValidToUpsert(data, true);
        assertTrue(r.isValid());
        assertEquals(HttpStatus.OK, r.getStatus());
        assertEquals("OK", r.getMessage());
    }

    @Test
    void isValidToUpsert_updateWithOneUpdatableField_returnsOK() {
        Map<String, Object> data = new HashMap<>();
        data.put("data", Map.of(
                "id", "S1",
                "policy_id", "P",
                "domain_name", "D",
                "name", "alice"
        ));

        ValidationResult r = validator.isValidToUpsert(data, false);
        assertTrue(r.isValid());
        assertEquals(HttpStatus.OK, r.getStatus());
        assertEquals("OK", r.getMessage());
    }

    @Test
    void isValidToUpsert_whenDataIsJsonString_unwrapsAndValidates() {
        String json = "{\"policy_id\":\"P1\",\"domain_name\":\"customers\",\"foo\":\"bar\"}";
        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("data", json);

        ValidationResult r = validator.isValidToUpsert(wrapper, true);
        assertTrue(r.isValid());
        assertEquals(HttpStatus.OK, r.getStatus());
    }
}
