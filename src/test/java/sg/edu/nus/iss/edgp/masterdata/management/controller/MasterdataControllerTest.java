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
        
    }

   

    @Test
    void testGetUploadedData_allData() {
        SearchRequest request = new SearchRequest();
        request.setDomainName("vendor");

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1", "name", "Sample"));
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(auditDTO);
        when(masterdataService.getAllData("11")).thenReturn(mockData);

        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData("Bearer token", request);

        assertEquals(200, response.getStatusCodeValue());
       
        
    }

    @Test
    void testGetUploadedData_byPolicyAndOrgId() {
        SearchRequest request = new SearchRequest();
        request.setDomainName("vendor");
        request.setPolicyId("POL123");
       

        List<Map<String, Object>> mockData = List.of(Map.of("id", "1", "name", "Filtered Vendor"));
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(auditDTO);
        
        ResponseEntity<APIResponse<List<Map<String, Object>>>> response =
                controller.getUploadedData("Bearer token", request);

        assertEquals(200, response.getStatusCodeValue());
        
    }
}
