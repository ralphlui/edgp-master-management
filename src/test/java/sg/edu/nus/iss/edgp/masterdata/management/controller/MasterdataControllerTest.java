package sg.edu.nus.iss.edgp.masterdata.management.controller;


import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import sg.edu.nus.iss.edgp.masterdata.management.dto.*;
import sg.edu.nus.iss.edgp.masterdata.management.enums.AuditLogInvalidUser;
import sg.edu.nus.iss.edgp.masterdata.management.enums.HTTPVerb;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;

@ExtendWith(SpringExtension.class)
public class MasterdataControllerTest {

    @InjectMocks
    private MasterdataController controller;

    @Mock
    private MasterdataService masterdataService;

    @Mock
    private AuditService auditService;

    @Value("${audit.activity.type.prefix:MDM}")
    private String activityTypePrefix = "MDM";

    private UploadRequest uploadRequest;
    private AuditDTO auditDTO;

    @BeforeEach
    void setUp() {
        auditDTO = new AuditDTO();
        uploadRequest = new UploadRequest();
        uploadRequest.setCategory("vendor");
    }

    @Test
    void testUploadAndInsertCsvData_success() throws Exception {
        String authorization = "Bearer token";
        String successMessage = "Upload successful";
        MockMultipartFile file = new MockMultipartFile("file", "test.csv", "text/csv", "id,name".getBytes());

        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(auditDTO);
        when(masterdataService.uploadCsvDataToTable(file, uploadRequest,authorization)).thenReturn(successMessage);

        ResponseEntity<APIResponse<String>> response = controller.uploadAndInsertCsvData(authorization, uploadRequest, file);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals(successMessage, response.getBody().getMessage());
    }

    @Test
    void testUploadAndInsertCsvData_failure() throws Exception {
        String authorization = "Bearer token";
        MockMultipartFile file = new MockMultipartFile("file", "test.csv", "text/csv", "id,name".getBytes());

        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(auditDTO);
        when(masterdataService.uploadCsvDataToTable(file, uploadRequest,authorization)).thenReturn("");

        ResponseEntity<APIResponse<String>> response = controller.uploadAndInsertCsvData(authorization, uploadRequest, file);

        assertEquals(404, response.getStatusCodeValue());
        assertTrue(response.getBody().getMessage().contains("Upload failed"));
    }

    @Test
    void testGetUploadedData_allData() {
        SearchRequest request = new SearchRequest();
        request.setCategory("vendor");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1", "name", "Sample"));
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(auditDTO);
        when(masterdataService.getAllData(request)).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData("Bearer token", request);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Successfully retrieved vendor data.", response.getBody().getMessage());
        
    }

    @Test
    void testGetUploadedData_byPolicyAndOrgId() {
        SearchRequest request = new SearchRequest();
        request.setCategory("vendor");
        request.setPolicyId("POL123");
        request.setOrganizationId("ORG456");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1", "name", "Filtered Vendor"));
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(auditDTO);
        when(masterdataService.getDataByPolicyAndOrgId(request)).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData("Bearer token", request);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Successfully retrieved vendor data.", response.getBody().getMessage());
    }
}
