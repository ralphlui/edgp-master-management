package sg.edu.nus.iss.edgp.masterdata.management.controller;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.masterdata.management.dto.*;
import sg.edu.nus.iss.edgp.masterdata.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DataUploadValidation;

@ExtendWith(SpringExtension.class)
public class MasterdataControllerTest {

    @InjectMocks
    private MasterdataController controller;

    @Mock
    private MasterdataService masterdataService;

    @Mock
    private AuditService auditService;

    @Mock
    private DataUploadValidation dataUploadValidation;

    @Value("${audit.activity.type.prefix:MDM}")
    private String activityTypePrefix = "MDM";

    private UploadRequest uploadRequest;
    private AuditDTO auditDTO;
    private static final String AUTH = "Bearer token";

    @BeforeEach
    void setUp() {
        auditDTO = new AuditDTO();
        uploadRequest = new UploadRequest();
        ReflectionTestUtils.setField(controller, "activityTypePrefix", "MDM");

        when(auditService.createAuditDTO(anyString(), anyString(), anyString(), anyString(), any(HTTPVerb.class)))
                .thenReturn(auditDTO);
    }

    private ValidationResult valid() {
        ValidationResult v = new ValidationResult();
        v.setValid(true);
        v.setStatus(HttpStatus.OK);
        v.setMessage("OK");
        return v;
    }

    private ValidationResult invalid(HttpStatus status, String msg) {
        ValidationResult v = new ValidationResult();
        v.setValid(false);
        v.setStatus(status);
        v.setMessage(msg);
        return v;
    }

    private UploadResult result(int total, String msg, List<Map<String,Object>> data) {
        UploadResult r = new UploadResult(msg, total, data);
        r.setTotalRecord(total);
        r.setMessage(msg);
        r.setData(data);
        return r;
    }


    @Test
    void testGetUploadedData_allData() {
        SearchRequest request = new SearchRequest();
        request.setDomainName(""); // important so trim() works & branch hits "all"
        request.setPolicyId("");
        request.setFileId("");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1", "name", "Sample"));
        when(masterdataService.getAllData(AUTH)).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData(AUTH, request);

        assertEquals(200, response.getStatusCodeValue());
        assertTrue(response.getBody().getSuccess());
        verify(masterdataService).getAllData(AUTH);
        verify(auditService).logAudit(eq(auditDTO), eq(200), contains("Successfully"), eq(AUTH));
    }

    @Test
    void testGetUploadedData_byPolicyAndDomain() {
        SearchRequest request = new SearchRequest();
        request.setDomainName("vendor");
        request.setPolicyId("POL123");
        request.setFileId("");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1", "name", "Filtered Vendor"));
        when(masterdataService.getDataByPolicyAndDomainName(eq(request), eq(AUTH))).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData(AUTH, request);

        assertEquals(200, response.getStatusCodeValue());
        assertTrue(response.getBody().getSuccess());
        verify(masterdataService).getDataByPolicyAndDomainName(eq(request), eq(AUTH));
    }

    @Test
    void testGetUploadedData_byPolicyOnly() {
        SearchRequest request = new SearchRequest();
        request.setDomainName("");
        request.setPolicyId("POL123");
        request.setFileId("");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1"));
        when(masterdataService.getDataByPolicyId(eq(request), eq(AUTH))).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData(AUTH, request);

        assertEquals(200, response.getStatusCodeValue());
        verify(masterdataService).getDataByPolicyId(eq(request), eq(AUTH));
    }

    @Test
    void testGetUploadedData_byDomainOnly() {
        SearchRequest request = new SearchRequest();
        request.setDomainName("vendor");
        request.setPolicyId("");
        request.setFileId("");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1"));
        when(masterdataService.getDataByDomainName(eq(request), eq(AUTH))).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData(AUTH, request);

        assertEquals(200, response.getStatusCodeValue());
        verify(masterdataService).getDataByDomainName(eq(request), eq(AUTH));
    }

    @Test
    void testGetUploadedData_byFileIdOnly() {
        SearchRequest request = new SearchRequest();
        request.setDomainName("");
        request.setPolicyId("");
        request.setFileId("FILE-1");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1"));
        when(masterdataService.getDataByFileId(eq(request), eq(AUTH))).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData(AUTH, request);

        assertEquals(200, response.getStatusCodeValue());
        verify(masterdataService).getDataByFileId(eq(request), eq(AUTH));
    }

    @Test
    void testGetUploadedData_serviceThrows() {
        SearchRequest request = new SearchRequest();
        request.setDomainName("");
        request.setPolicyId("");
        request.setFileId("");

        when(masterdataService.getAllData(AUTH)).thenThrow(new RuntimeException("boom"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData(AUTH, request);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        assertEquals("An unexpected error occurred. Please contact support.", response.getBody().getMessage());
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("unexpected error"), eq(AUTH));
    }

    // =========================================================
    // getUploadedFile
    // =========================================================

    @Test
    void testGetUploadedFile_success() {
        List<Map<String, Object>> mockData = List.of(Map.of("fileId", "F1"));
        when(masterdataService.getAllUploadFiles(AUTH)).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedFile(AUTH);

        assertEquals(200, response.getStatusCodeValue());
        assertTrue(response.getBody().getSuccess());
        verify(auditService).logAudit(eq(auditDTO), eq(200), contains("Successfully"), eq(AUTH));
    }

    @Test
    void testGetUploadedFile_error() {
        when(masterdataService.getAllUploadFiles(AUTH)).thenThrow(new RuntimeException("x"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedFile(AUTH);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("unexpected error"), eq(AUTH));
    }
 

    @Test
    void testUploadAndInsertCsvData_success() {
        MockMultipartFile file = new MockMultipartFile(
                "file", "vendors.csv", "text/csv",
                "id,name\n1,Acme".getBytes(StandardCharsets.UTF_8));

        when(dataUploadValidation.isValidToUpload(eq(file), eq(uploadRequest), eq(AUTH))).thenReturn(valid());
        List<Map<String, Object>> data = List.of(Map.of("id", "1", "name", "Acme"));
        when(masterdataService.uploadCsvDataToTable(eq(file), eq(uploadRequest), eq(AUTH)))
                .thenReturn(result(1, "Inserted 1 record", data));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.uploadAndInsertCsvData(AUTH, uploadRequest, file);

        assertEquals(200, response.getStatusCodeValue());
        assertTrue(response.getBody().getSuccess());
       
    }

    @Test
    void testUploadAndInsertCsvData_validationFails() {
        MockMultipartFile file = new MockMultipartFile("file", "f.csv", "text/csv", new byte[0]);

        when(dataUploadValidation.isValidToUpload(eq(file), eq(uploadRequest), eq(AUTH)))
                .thenReturn(invalid(HttpStatus.BAD_REQUEST, "Missing file"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.uploadAndInsertCsvData(AUTH, uploadRequest, file);

        assertEquals(400, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        verify(masterdataService, never()).uploadCsvDataToTable(any(), any(), any());
        verify(auditService).logAudit(eq(auditDTO), eq(400), contains("Missing file"), eq(AUTH));
    }

    @Test
    void testUploadAndInsertCsvData_zeroRows() {
        MockMultipartFile file = new MockMultipartFile("file", "f.csv", "text/csv", "x".getBytes());
        when(dataUploadValidation.isValidToUpload(eq(file), eq(uploadRequest), eq(AUTH))).thenReturn(valid());
        when(masterdataService.uploadCsvDataToTable(eq(file), eq(uploadRequest), eq(AUTH)))
                .thenReturn(result(0, "No rows", List.of()));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.uploadAndInsertCsvData(AUTH, uploadRequest, file);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("Upload failed"), eq(AUTH));
    }

    @Test
    void testUploadAndInsertCsvData_serviceThrowsCustom() {
        MockMultipartFile file = new MockMultipartFile("file", "f.csv", "text/csv", "x".getBytes());
        when(dataUploadValidation.isValidToUpload(eq(file), eq(uploadRequest), eq(AUTH))).thenReturn(valid());
        when(masterdataService.uploadCsvDataToTable(eq(file), eq(uploadRequest), eq(AUTH)))
                .thenThrow(new MasterdataServiceException("Boom"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.uploadAndInsertCsvData(AUTH, uploadRequest, file);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        assertTrue(response.getBody().getMessage().contains("Boom"));
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("Boom"), eq(AUTH));
    }

    @Test
    void testUploadAndInsertCsvData_serviceThrowsGeneric() {
        MockMultipartFile file = new MockMultipartFile("file", "f.csv", "text/csv", "x".getBytes());
        when(dataUploadValidation.isValidToUpload(eq(file), eq(uploadRequest), eq(AUTH))).thenReturn(valid());
        when(masterdataService.uploadCsvDataToTable(eq(file), eq(uploadRequest), eq(AUTH)))
                .thenThrow(new RuntimeException("NPE"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.uploadAndInsertCsvData(AUTH, uploadRequest, file);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        assertEquals("An unexpected error occurred. Please contact support.", response.getBody().getMessage());
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("unexpected error"), eq(AUTH));
    }
 

    @Test
    void testUpdateData_success() {
        Map<String, Object> payload = Map.of("id", "1", "name", "Acme");
        when(dataUploadValidation.isValidToUpsert(eq(payload), eq(false))).thenReturn(valid());
        List<Map<String, Object>> data = List.of(Map.of("id", "1", "name", "Acme"));
        when(masterdataService.updateDataToTable(eq(payload))).thenReturn(result(1, "Updated 1 row", data));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.updateData(AUTH, payload);

        assertEquals(200, response.getStatusCodeValue());
        assertTrue(response.getBody().getSuccess());
        
    }

    @Test
    void testUpdateData_validationFails() {
        Map<String, Object> payload = Map.of("id", "1");
        when(dataUploadValidation.isValidToUpsert(eq(payload), eq(false)))
                .thenReturn(invalid(HttpStatus.BAD_REQUEST, "Invalid payload"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.updateData(AUTH, payload);

        assertEquals(400, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        verify(masterdataService, never()).updateDataToTable(any());
        verify(auditService).logAudit(eq(auditDTO), eq(400), contains("Invalid payload"), eq(AUTH));
    }

    @Test
    void testUpdateData_zeroRows() {
        Map<String, Object> payload = Map.of("id", "1");
        when(dataUploadValidation.isValidToUpsert(eq(payload), eq(false))).thenReturn(valid());
        when(masterdataService.updateDataToTable(eq(payload))).thenReturn(result(0, "No updates", List.of()));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.updateData(AUTH, payload);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("No updates"), eq(AUTH));
    }

    @Test
    void testUpdateData_serviceThrowsCustom() {
        Map<String, Object> payload = Map.of("id", "1");
        when(dataUploadValidation.isValidToUpsert(eq(payload), eq(false))).thenReturn(valid());
        when(masterdataService.updateDataToTable(eq(payload)))
                .thenThrow(new MasterdataServiceException("Bad update"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.updateData(AUTH, payload);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        assertTrue(response.getBody().getMessage().contains("Bad update"));
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("Bad update"), eq(AUTH));
    }

    @Test
    void testUpdateData_serviceThrowsGeneric() {
        Map<String, Object> payload = Map.of("id", "1");
        when(dataUploadValidation.isValidToUpsert(eq(payload), eq(false))).thenReturn(valid());
        when(masterdataService.updateDataToTable(eq(payload)))
                .thenThrow(new RuntimeException("boom"));

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.updateData(AUTH, payload);

        assertEquals(500, response.getStatusCodeValue());
        assertFalse(response.getBody().getSuccess());
        assertEquals("An unexpected error occurred. Please contact support.", response.getBody().getMessage());
        verify(auditService).logAudit(eq(auditDTO), eq(500), contains("unexpected error"), eq(AUTH));
    }
}
