package sg.edu.nus.iss.edgp.masterdata.management.service;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.dto.InsertionSummary;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.StagingDataService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@ExtendWith(MockitoExtension.class)
public class StagingDataServiceTest {

    private DynamoDbClient dynamoDbClient;
    private StagingDataService svc;

    @BeforeEach
    void setUp() {
        dynamoDbClient = mock(DynamoDbClient.class);
        svc = new StagingDataService(dynamoDbClient);
    }

    

    @Test
    void insertToStaging_nullTable_throwsNPE() {
        List<LinkedHashMap<String, Object>> rows = List.of(new LinkedHashMap<>(Map.of("a", "b")));
        assertThrows(NullPointerException.class,
                () -> svc.insertToStaging(null, rows, "ORG", "POL", "customer", "F1", "u@x.com"));
        verifyNoInteractions(dynamoDbClient);
    }

    @Test
    void insertToStaging_blankDomain_throwsIAE() {
        List<LinkedHashMap<String, Object>> rows = List.of(new LinkedHashMap<>(Map.of("a", "b")));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> svc.insertToStaging("stage", rows, "ORG", "POL", "  ", "F1", "u@x.com"));
        assertEquals("domain_name is mandatory", ex.getMessage());
        verifyNoInteractions(dynamoDbClient);
    }

    

    @Test
    void insertToStaging_oneRow_mapsTypes_addsMeta_buildsBatch() {
        
        when(dynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Collections.emptyMap()).build());

        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put(" full name ", "Alice");
        row.put("age", "42");
        row.put("price", "3.14");
        row.put("active", "true");
        row.put("tags", List.of("A", "B", 3));
        row.put("meta%", Map.of("a", 1, "b", "x"));
        row.put("empty", "");

        InsertionSummary sum = svc.insertToStaging(
                "stage",
                List.of(row),
                "ORG1",
                "POL1",
                "customer",
                "F-1",
                "user@x.com"
        );
 
        ArgumentCaptor<BatchWriteItemRequest> cap = ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(dynamoDbClient, times(1)).batchWriteItem(cap.capture());

        BatchWriteItemRequest req = cap.getValue();
        assertEquals(Set.of("stage"), req.requestItems().keySet());
        List<WriteRequest> wrs = req.requestItems().get("stage");
        assertEquals(1, wrs.size());
        PutRequest put = wrs.get(0).putRequest();
        Map<String, ?> item = put.item();
 
        assertTrue(item.containsKey("id"));
        assertNotNull(put.item().get("id").s());

        
        assertTrue(put.item().containsKey("full_name"));
        assertTrue(put.item().containsKey("meta_"));
        assertFalse(put.item().containsKey(" full name "));
        assertFalse(put.item().containsKey("meta%"));
 
        assertEquals("ORG1", put.item().get("organization_id").s());
        assertEquals("POL1", put.item().get("policy_id").s());
        assertEquals("customer", put.item().get("domain_name").s());
        assertEquals("F-1", put.item().get("file_id").s());
        assertEquals("user@x.com", put.item().get("uploaded_by").s());
        assertNotNull(put.item().get("uploaded_date").s());
        assertEquals("0", put.item().get("is_processed").n());
        assertEquals("0", put.item().get("is_handled").n());
 
        assertEquals("Alice", put.item().get("full_name").s());
        assertEquals("42", put.item().get("age").n());
        assertEquals("3.14", put.item().get("price").n());
        assertTrue(put.item().get("active").bool());
        assertTrue(put.item().get("tags").hasL());
        assertTrue(put.item().get("meta_").hasM());
        assertFalse(put.item().containsKey("empty"));

        
        assertEquals(1, sum.totalInserted());
        assertEquals(1, sum.previewTop50().size());
        Map<String, Object> prev = sum.previewTop50().get(0);
        
        assertEquals(new BigDecimal("42"), prev.get("age"));
        assertEquals(new BigDecimal("3.14"), prev.get("price"));
        assertEquals(true, prev.get("active"));
         
        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) prev.get("tags");
        assertEquals(3, tags.size());
    }

    @Test
    void insertToStaging_missingOptionalMetaFields_areOmitted() {
        when(dynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Collections.emptyMap()).build());

        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("a", 1);

        InsertionSummary sum = svc.insertToStaging(
                "stage",
                List.of(row),
                null,      
                "",
                "customer",
                "  ",
                null
        );

        assertEquals(1, sum.totalInserted());
        ArgumentCaptor<BatchWriteItemRequest> cap = ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(dynamoDbClient).batchWriteItem(cap.capture());

        Map<String, ?> item = cap.getValue().requestItems().get("stage").get(0).putRequest().item();
        assertFalse(item.containsKey("organization_id"));
        assertFalse(item.containsKey("policy_id"));
        assertFalse(item.containsKey("file_id"));
        assertFalse(item.containsKey("uploaded_by"));
        assertTrue(item.containsKey("domain_name")); // mandatory
    }

    

    @Test
    void insertToStaging_26Rows_flushesTwice_andReturns26() {
        when(dynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Collections.emptyMap()).build());

        List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 26; i++) {
            rows.add(new LinkedHashMap<>(Map.of("k", "v" + i)));
        }

        InsertionSummary sum = svc.insertToStaging("stage", rows, "O", "P", "customer", "F", "u");

        assertEquals(26, sum.totalInserted());
        assertEquals(26, sum.previewTop50().size()); 

         
        ArgumentCaptor<BatchWriteItemRequest> cap = ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(dynamoDbClient, times(2)).batchWriteItem(cap.capture());

        List<BatchWriteItemRequest> calls = cap.getAllValues();
         
        int first = calls.get(0).requestItems().get("stage").size();
        int second = calls.get(1).requestItems().get("stage").size();
        assertEquals(25, first);
        assertEquals(1, second);
    }

    @Test
    void insertToStaging_previewIsCappedAt50_whenManyRows() {
        when(dynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Collections.emptyMap()).build());

        List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            rows.add(new LinkedHashMap<>(Map.of("i", i)));
        }

        InsertionSummary sum = svc.insertToStaging("stage", rows, "O", "P", "customer", "F", "u");

        assertEquals(60, sum.totalInserted());
        assertEquals(50, sum.previewTop50().size());

        
        BigDecimal firstI = (BigDecimal) sum.previewTop50().get(0).get("i");
        assertEquals(new BigDecimal("0"), firstI);
    }

    

    @Test
    void insertToStaging_numericStringWithExponent_storedAsNumber() {
        when(dynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().unprocessedItems(Collections.emptyMap()).build());

        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("exp", "1e-3");

        svc.insertToStaging("stage", List.of(row), "O", "P", "customer", "F", "u");

        ArgumentCaptor<BatchWriteItemRequest> cap = ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(dynamoDbClient).batchWriteItem(cap.capture());

        Map<String, ?> item = cap.getValue().requestItems().get("stage").get(0).putRequest().item();
        assertEquals("1e-3", ((AttributeValue) item.get("exp")).n());
    }
}

