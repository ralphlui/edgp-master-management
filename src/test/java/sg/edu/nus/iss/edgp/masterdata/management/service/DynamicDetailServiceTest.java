package sg.edu.nus.iss.edgp.masterdata.management.service;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@ExtendWith(MockitoExtension.class)
public class DynamicDetailServiceTest {

    private DynamoDbClient dynamoDbClient;
    private DynamicDetailService svc;

    @BeforeEach
    void setUp() {
        dynamoDbClient = mock(DynamoDbClient.class);
        svc = new DynamicDetailService(dynamoDbClient);
    }


    @Test
    void insertStagingMasterData_nullOrEmpty_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> svc.insertStagingMasterData("tbl", null));
        assertThrows(IllegalArgumentException.class, () -> svc.insertStagingMasterData("tbl", Collections.emptyMap()));
        verifyNoInteractions(dynamoDbClient);
    }

    @Test
    void insertStagingMasterData_generatesId_andConvertsTypes() {
        Map<String, String> raw = new LinkedHashMap<>();

        raw.put("name", " Alice ");
        raw.put("age", "42");
        raw.put("price", "3.14");
        raw.put("active", "true");
        raw.put("empty", "  ");

        svc.insertStagingMasterData("stage", raw);

        ArgumentCaptor<PutItemRequest> cap = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(cap.capture());

        PutItemRequest req = cap.getValue();
        assertEquals("stage", req.tableName());

        Map<String, AttributeValue> item = req.item();
        assertNotNull(item.get("id"));
        assertNotNull(item.get("id").s()); // generated UUID
        assertEquals("Alice", item.get("name").s());
        assertEquals("42", item.get("age").n());
        assertEquals("3.14", item.get("price").n());
        assertTrue(item.get("active").bool());
        assertEquals(Boolean.TRUE, item.get("empty").nul());
    }

    @Test
    void insertStagingMasterData_respectsProvidedId_overwritesOnce() {
        Map<String, String> raw = new LinkedHashMap<>();
        raw.put("id", "ID-1");
        raw.put("note", "hi");

        svc.insertStagingMasterData("stage", raw);

        ArgumentCaptor<PutItemRequest> cap = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(cap.capture());

        Map<String, AttributeValue> item = cap.getValue().item();
        assertEquals("ID-1", item.get("id").s());
        assertEquals("hi", item.get("note").s());
    }

    @Test
    void tableExists_true_onDescribeOk() {
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(DescribeTableResponse.builder().build());
        assertTrue(svc.tableExists("t"));
    }

    @Test
    void tableExists_false_onResourceNotFound() {
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("nope").build());
        assertFalse(svc.tableExists("t"));
    }


    @Test
    void createTable_buildsSchema_andWaitsActiveOnce() {
    	
        when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(DescribeTableResponse.builder()
                        .table(TableDescription.builder()
                                .tableStatus(TableStatus.ACTIVE)
                                .build())
                        .build());

        svc.createTable("new_tbl");

        ArgumentCaptor<CreateTableRequest> cap = ArgumentCaptor.forClass(CreateTableRequest.class);
        verify(dynamoDbClient).createTable(cap.capture());
        CreateTableRequest ctr = cap.getValue();

        assertEquals("new_tbl", ctr.tableName());
        assertEquals(BillingMode.PAY_PER_REQUEST, ctr.billingMode());
        assertEquals(1, ctr.keySchema().size());
        assertEquals("id", ctr.keySchema().get(0).attributeName());
        assertEquals(KeyType.HASH, ctr.keySchema().get(0).keyType());
        assertEquals(1, ctr.attributeDefinitions().size());
        assertEquals("id", ctr.attributeDefinitions().get(0).attributeName());
        assertEquals(ScalarAttributeType.S, ctr.attributeDefinitions().get(0).attributeType());

        verify(dynamoDbClient, atLeastOnce()).describeTable(any(DescribeTableRequest.class));
    }

  

    @Test
    void insertValidatedMasterData_empty_noCall() {
        svc.insertValidatedMasterData("t", null);
        svc.insertValidatedMasterData("t", Collections.emptyMap());
        verifyNoInteractions(dynamoDbClient);
    }

    @Test
    void insertValidatedMasterData_nonEmpty_putsItem() {
        Map<String, AttributeValue> row = Map.of("id", AttributeValue.builder().s("X").build());
        svc.insertValidatedMasterData("track", row);

        ArgumentCaptor<PutItemRequest> cap = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(cap.capture());
        assertEquals("track", cap.getValue().tableName());
        assertEquals("X", cap.getValue().item().get("id").s());
    }

    
    @Test
    void getUnprocessedRecordsByFileId_returnsItems() {
        Map<String, AttributeValue> it = Map.of("id", AttributeValue.builder().s("S1").build());
        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(it).build());

        List<Map<String, AttributeValue>> out =
                svc.getUnprocessedRecordsByFileId("stage", "F1", "P1", "customer");

        assertEquals(1, out.size());

        ArgumentCaptor<ScanRequest> cap = ArgumentCaptor.forClass(ScanRequest.class);
        verify(dynamoDbClient).scan(cap.capture());
        ScanRequest sr = cap.getValue();
        assertEquals("stage", sr.tableName());
        assertTrue(sr.filterExpression().contains("file_id = :file_id"));
        assertTrue(sr.filterExpression().contains("domain_name = :domain_name"));
        assertTrue(sr.filterExpression().contains("policy_id = :policy_id"));
        assertTrue(sr.filterExpression().contains("is_processed = :is_processed"));
        assertTrue(sr.filterExpression().contains("is_handled= :is_handled"));
        assertEquals("F1", sr.expressionAttributeValues().get(":file_id").s());
        assertEquals("customer", sr.expressionAttributeValues().get(":domain_name").s());
        assertEquals("P1", sr.expressionAttributeValues().get(":policy_id").s());
        assertEquals("0", sr.expressionAttributeValues().get(":is_processed").n());
        assertEquals("0", sr.expressionAttributeValues().get(":is_handled").n());
    }

    @Test
    void getUnprocessedRecordsByFileId_empty_throwsRuntime() {
        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(new ArrayList<>()).build());
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> svc.getUnprocessedRecordsByFileId("stage", "F1", "P1", "customer"));
        assertTrue(ex.getMessage().contains("Data not found"));
    }


    @Test
    void updateStagingProcessedStatus_buildsUpdateCorrectly() {
        svc.updateStagingProcessedStatus("hdr", "F-1", "1");

        ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(cap.capture());
        UpdateItemRequest ur = cap.getValue();

        assertEquals("hdr", ur.tableName());
        assertEquals("F-1", ur.key().get("id").s());
        assertEquals("SET is_processed = :s, updated_date = :ud", ur.updateExpression());
        assertEquals("attribute_exists(id)", ur.conditionExpression());
        assertEquals("1", ur.expressionAttributeValues().get(":s").s());
        assertNotNull(ur.expressionAttributeValues().get(":ud").s()); // timestamp present
    }


    @Test
    void getDomainNameByFileID_returnsItem() {
        Map<String, AttributeValue> item = Map.of("domain_name", AttributeValue.builder().s("customer").build());
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        Map<String, AttributeValue> out = svc.getDomainNameByFileID("hdr", "F-1");
        assertEquals("customer", out.get("domain_name").s());
    }

    @Test
    void getDomainNameByFileID_onDdbException_returnsNull() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("boom").build());
        assertNull(svc.getDomainNameByFileID("hdr", "F-1"));
    }


    @Test
    void claimStagingRow_success_returnsTrue_andBuildsRequest() {
      
        boolean ok = svc.claimStagingRow("stage", "S1");
        assertTrue(ok);

        ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(cap.capture());
        UpdateItemRequest ur = cap.getValue();
        assertEquals("stage", ur.tableName());
        assertEquals("S1", ur.key().get("id").s());
        assertTrue(ur.conditionExpression().contains("is_handled = :zero") || ur.conditionExpression().contains("is_handled = :zero"));
        assertTrue(ur.updateExpression().contains("SET is_handled = :one"));
        assertNotNull(ur.expressionAttributeValues().get(":one"));
        assertNotNull(ur.expressionAttributeValues().get(":zero"));
        assertNotNull(ur.expressionAttributeValues().get(":nowStr"));
        assertNotNull(ur.expressionAttributeValues().get(":nowTs"));
    }

    @Test
    void claimStagingRow_conditionFailed_returnsFalse() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder().message("claimed").build());
        assertFalse(svc.claimStagingRow("stage", "S1"));
    }

    @Test
    void markProcessed_buildsUpdateCorrectly() {
        svc.markProcessed("stage", "S1");

        ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(cap.capture());
        UpdateItemRequest ur = cap.getValue();
        assertEquals("stage", ur.tableName());
        assertEquals("S1", ur.key().get("id").s());
        assertTrue(ur.conditionExpression().startsWith("is_handled = :one"));
        assertTrue(ur.updateExpression().contains("SET is_processed = :one"));
        assertNotNull(ur.expressionAttributeValues().get(":nowTs"));
        assertNotNull(ur.expressionAttributeValues().get(":nowStr"));
        assertEquals("1", ur.expressionAttributeValues().get(":one").n());
        assertEquals("0", ur.expressionAttributeValues().get(":zero").n());
    }

    @Test
    void revertClaim_buildsUpdateCorrectly() {
        svc.revertClaim("stage", "S1");

        ArgumentCaptor<UpdateItemRequest> cap = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(cap.capture());
        UpdateItemRequest ur = cap.getValue();
        assertEquals("stage", ur.tableName());
        assertEquals("S1", ur.key().get("id").s());
        assertTrue(ur.conditionExpression().startsWith("is_handled = :one"));
        assertTrue(ur.updateExpression().startsWith("SET is_handled = :zero"));
        assertNotNull(ur.expressionAttributeValues().get(":one"));
        assertNotNull(ur.expressionAttributeValues().get(":zero"));
    }
}
