package sg.edu.nus.iss.edgp.masterdata.management.controller;

import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.AuditService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DomainService;

@WebMvcTest(DomainController.class)
@AutoConfigureMockMvc(addFilters = false)
class DomainControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DomainService domainService;

    @MockitoBean
    private JWTService jwtService;

    @MockitoBean
    private AuditService auditService;

   

    @Test
    @WithMockUser(authorities = {"SCOPE_view:mdm"})
    void getAllCategory_shouldReturnSuccess() throws Exception {
        List<String> mockCategories = List.of("Vendor", "Product", "Customer");

        when(domainService.findDomains()).thenReturn(mockCategories);
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        mockMvc.perform(get("/api/mdm/tables/domains")
                        .header("Authorization", "Bearer mock-token")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data[0]").value("Vendor"))
                .andExpect(jsonPath("$.totalRecord").value(3));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_view:mdm"})
    void getAllCategory_shouldReturnEmptyListMessage() throws Exception {
        when(domainService.findDomains()).thenReturn(List.of());
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        mockMvc.perform(get("/api/mdm/tables/domains")
                        .header("Authorization", "Bearer mock-token")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.totalRecord").value(0))
                .andExpect(jsonPath("$.message").value("No data found."));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_view:mdm"})
    void getAllCategory_shouldReturn500OnException() throws Exception {
        when(domainService.findDomains()).thenThrow(new RuntimeException("Dynamo error"));
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        mockMvc.perform(get("/api/mdm/tables/domains")
                        .header("Authorization", "Bearer mock-token")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("An unexpected error occurred. Please contact support."));
    }

  

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createDomain_shouldReturn200OnSuccess() throws Exception {
        when(domainService.createDomain(eq("Vendor"))).thenReturn(true);
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        mockMvc.perform(post("/api/mdm/tables/domain/create")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Vendor\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("Domain successfully created "));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createDomain_shouldReturn400WhenNameMissing_null() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

      
        mockMvc.perform(post("/api/mdm/tables/domain/create")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("Domain Name is required"));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createDomain_shouldReturn400WhenNameBlank() throws Exception {
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        mockMvc.perform(post("/api/mdm/tables/domain/create")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"   \"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("Domain Name is required"));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createDomain_shouldReturn500WhenServiceReturnsFalse() throws Exception {
        when(domainService.createDomain(eq("Vendor"))).thenReturn(false);
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        mockMvc.perform(post("/api/mdm/tables/domain/create")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Vendor\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("Domain creation failed"));
    }

    @Test
    @WithMockUser(authorities = {"SCOPE_manage:mdm"})
    void createDomain_shouldReturn500OnException() throws Exception {
        when(domainService.createDomain(anyString())).thenThrow(new RuntimeException("boom"));
        when(auditService.createAuditDTO(any(), any(), any(), any(), any())).thenReturn(new AuditDTO());

        mockMvc.perform(post("/api/mdm/tables/domain/create")
                        .header("Authorization", "Bearer mock-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Vendor\"}")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value("An unexpected error occurred. Please contact support."));
    }
}
