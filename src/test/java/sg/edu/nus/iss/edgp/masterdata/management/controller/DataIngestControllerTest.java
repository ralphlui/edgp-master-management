package sg.edu.nus.iss.edgp.masterdata.management.controller;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.dto.ValidationResult;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DataIngestService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.DataUploadValidation;

@WebMvcTest(DataIngestController.class)
@AutoConfigureMockMvc(addFilters = false)
class DataIngestControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DataIngestService dataIngestService;

    @MockitoBean
    private DataUploadValidation dataUploadValidation;

    @MockitoBean
    private AuditService auditService;

    
    @MockitoBean
    private JWTService jwtService;

  

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createData_shouldReturn200OnSuccess() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult ok = new ValidationResult();
        ok.setValid(true);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(true))).thenReturn(ok);

        UploadResult res = new UploadResult("", 0, null);
        res.setTotalRecord(1);
        res.setMessage("Inserted 1 row");
        res.setData(List.of(Map.of("id", "1")));
        when(dataIngestService.processIngest(anyMap(), anyString())).thenReturn(res);

        mockMvc.perform(post("/api/mdm/data/ingest")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\",\"name\":\"Acme\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("Inserted 1 row"))
                .andExpect(jsonPath("$.totalRecord").value(1));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createData_shouldReturn400OnValidationFail() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult bad = new ValidationResult();
        bad.setValid(false);
        bad.setMessage("Invalid payload");
        bad.setStatus(org.springframework.http.HttpStatus.BAD_REQUEST);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(true))).thenReturn(bad);

        mockMvc.perform(post("/api/mdm/data/ingest")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("Invalid payload"));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createData_shouldReturn500WhenServiceReturnsZero() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult ok = new ValidationResult();
        ok.setValid(true);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(true))).thenReturn(ok);

        UploadResult res = new UploadResult("", 0, null);
        res.setTotalRecord(0);
        res.setMessage("No rows inserted");
        res.setData(List.of());
        when(dataIngestService.processIngest(anyMap(), anyString())).thenReturn(res);

        mockMvc.perform(post("/api/mdm/data/ingest")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\",\"name\":\"Acme\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("No rows inserted"));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createData_shouldReturn500OnException() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult ok = new ValidationResult();
        ok.setValid(true);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(true))).thenReturn(ok);

        when(dataIngestService.processIngest(anyMap(), anyString())).thenThrow(new RuntimeException("boom"));

        mockMvc.perform(post("/api/mdm/data/ingest")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\",\"name\":\"Acme\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("An unexpected error occurred. Please contact support."));
    }

  

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void updateData_shouldReturn200OnSuccess() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult ok = new ValidationResult();
        ok.setValid(true);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(false))).thenReturn(ok);

        UploadResult res = new UploadResult("", 0, null);
        res.setTotalRecord(1);
        res.setMessage("Updated 1 row");
        res.setData(List.of(Map.of("id", "1")));
        when(dataIngestService.updateDataToTable(anyMap(), anyString())).thenReturn(res);

        mockMvc.perform(put("/api/mdm/data/ingest/update")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\",\"name\":\"Acme\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("Updated 1 row"))
                .andExpect(jsonPath("$.totalRecord").value(1));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void updateData_shouldReturn400OnValidationFail() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult bad = new ValidationResult();
        bad.setValid(false);
        bad.setMessage("Invalid payload");
        bad.setStatus(org.springframework.http.HttpStatus.BAD_REQUEST);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(false))).thenReturn(bad);

        mockMvc.perform(put("/api/mdm/data/ingest/update")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("Invalid payload"));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void updateData_shouldReturn500WhenServiceReturnsZero() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult ok = new ValidationResult();
        ok.setValid(true);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(false))).thenReturn(ok);

        UploadResult res = new UploadResult("", 0, null);
        res.setTotalRecord(0);
        res.setMessage("No updates");
        res.setData(List.of());
        when(dataIngestService.updateDataToTable(anyMap(), anyString())).thenReturn(res);

        mockMvc.perform(put("/api/mdm/data/ingest/update")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\",\"name\":\"Acme\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("No updates"));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void updateData_shouldReturn500OnException() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        ValidationResult ok = new ValidationResult();
        ok.setValid(true);
        when(dataUploadValidation.isValidToUpsert(anyMap(), eq(false))).thenReturn(ok);

        when(dataIngestService.updateDataToTable(anyMap(), anyString())).thenThrow(new RuntimeException("boom"));

        mockMvc.perform(put("/api/mdm/data/ingest/update")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"1\",\"name\":\"Acme\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("An unexpected error occurred. Please contact support."));
    }
}
