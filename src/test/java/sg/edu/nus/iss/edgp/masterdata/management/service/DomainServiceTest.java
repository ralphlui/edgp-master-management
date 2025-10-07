package sg.edu.nus.iss.edgp.masterdata.management.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DomainService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@ExtendWith(MockitoExtension.class)
public class DomainServiceTest {

    private DynamoDbClient dynamoDbClient;
    private DynamicDetailService dynamoService;
    private DomainService svc;

    @BeforeEach
    void setUp() {
        dynamoDbClient = mock(DynamoDbClient.class);
        dynamoService = mock(DynamicDetailService.class);
        svc = new DomainService(dynamoDbClient, dynamoService);

 
        ReflectionTestUtils.setField(svc, "domainTableName", "domain_tbl");
    }

   

    @Test
    void findDomains_tableMissing_returnsEmptyList() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(false);

        List<String> out = svc.findDomains();
        assertTrue(out.isEmpty());

        verify(dynamoService).tableExists("domain_tbl");
        verifyNoInteractions(dynamoDbClient);
    }

    @Test
    void findDomains_happy_returnsNamesAndUsesAttributesToGet() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(true);

        Map<String, AttributeValue> it1 = Map.of("name", AttributeValue.builder().s("customer").build());
        Map<String, AttributeValue> it2 = Map.of("name", AttributeValue.builder().s("product").build());
        Map<String, AttributeValue> it3 = Map.of("other", AttributeValue.builder().s("ignore").build()); // no "name"

        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(it1, it2, it3).build());

        List<String> out = svc.findDomains();
        assertEquals(2, out.size());
        assertTrue(out.contains("customer"));
        assertTrue(out.contains("product"));

        ArgumentCaptor<ScanRequest> cap = ArgumentCaptor.forClass(ScanRequest.class);
        verify(dynamoDbClient).scan(cap.capture());
        ScanRequest sent = cap.getValue();
        assertEquals("domain_tbl", sent.tableName());
        assertEquals(List.of("name"), sent.attributesToGet());
    }

    @Test
    void findDomains_scanThrows_wrapsInMasterdataServiceException() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(true);
        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenThrow(DynamoDbException.builder().message("ddb down").build());

        MasterdataServiceException ex =
                assertThrows(MasterdataServiceException.class, () -> svc.findDomains());
        assertTrue(ex.getMessage().contains("fetching domains"));
    }
 
    @Test
    void createDomain_invalidInput_throwsWrappedRuntimeWithIAECause() {
        RuntimeException ex1 = assertThrows(RuntimeException.class, () -> svc.createDomain(null));
        assertTrue(ex1.getMessage().startsWith("Unexpected error in createDomain"));
        assertTrue(ex1.getCause() instanceof IllegalArgumentException);
        assertEquals("domainName is required", ex1.getCause().getMessage());

        RuntimeException ex2 = assertThrows(RuntimeException.class, () -> svc.createDomain("   "));
        assertTrue(ex2.getMessage().startsWith("Unexpected error in createDomain"));
        assertTrue(ex2.getCause() instanceof IllegalArgumentException);
        assertEquals("domainName is required", ex2.getCause().getMessage());
    }


    @Test
    void createDomain_success_tableCreatedIfMissing_andConditionalPut() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(false);

        boolean created = svc.createDomain("  customer  ");
        assertTrue(created);

        verify(dynamoService).createTable("domain_tbl");

        ArgumentCaptor<PutItemRequest> cap = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(cap.capture());
        PutItemRequest req = cap.getValue();

        assertEquals("domain_tbl", req.tableName());
        assertEquals("attribute_not_exists(#n)", req.conditionExpression());
        assertEquals("name", req.expressionAttributeNames().get("#n"));

        assertEquals("customer", req.item().get("name").s());
        String createdDate = req.item().get("createdDate").s();
        assertNotNull(createdDate);
        assertTrue(Pattern.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", createdDate));
    }

    @Test
    void createDomain_tableAlreadyExists_noCreateTable_called() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(true);

        boolean created = svc.createDomain("orders");
        assertTrue(created);

        verify(dynamoService, never()).createTable(anyString());

        ArgumentCaptor<PutItemRequest> cap = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(cap.capture());
        assertEquals("orders", cap.getValue().item().get("name").s());
    }

    @Test
    void createDomain_duplicate_returnsFalse_onConditionalCheckFailed() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(true);
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder().message("exists").build());

        boolean created = svc.createDomain("customer");
        assertFalse(created);
    }

    @Test
    void createDomain_ddbError_wrappedInRuntimeException() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(true);
        DynamoDbException ddb = (DynamoDbException) DynamoDbException.builder().message("boom").build();
        when(dynamoDbClient.putItem(any(PutItemRequest.class))).thenThrow(ddb);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> svc.createDomain("x"));
        assertTrue(ex.getMessage().contains("Failed to create domain"));
        assertSame(ddb, ex.getCause());
    }

    @Test
    void createDomain_sdkClientError_wrappedInRuntimeException() {
        when(dynamoService.tableExists("domain_tbl")).thenReturn(true);
        SdkClientException sce = SdkClientException.create("net err");
        when(dynamoDbClient.putItem(any(PutItemRequest.class))).thenThrow(sce);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> svc.createDomain("y"));
        assertTrue(ex.getMessage().contains("Failed to create domain"));
        assertSame(sce, ex.getCause());
    }
}


