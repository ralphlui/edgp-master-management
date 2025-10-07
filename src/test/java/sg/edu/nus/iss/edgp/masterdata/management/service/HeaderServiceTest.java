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

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

@ExtendWith(MockitoExtension.class)
class HeaderServiceTest {

    private DynamoDbClient ddb;
    private HeaderService service;

    @BeforeEach
    void setUp() {
        ddb = mock(DynamoDbClient.class);
        service = new HeaderService(ddb);
        
        ReflectionTestUtils.setField(service, "headerTableName", "md_header");
    }


    @Test
    void saveHeader_putsItemWithExpectedAttributes() {
        MasterDataHeader hdr = new MasterDataHeader();
        hdr.setId("H1");
        hdr.setFileName("file.csv");
        hdr.setDomainName("customer");
        hdr.setOrganizationId("ORG1");
        hdr.setPolicyId("POL1");
        hdr.setUploadedBy("user@example.com");
        hdr.setTotalRowsCount(5);
        hdr.setProcessStage(FileProcessStage.UNPROCESSED);
        hdr.setFileStatus("NEW");

        service.saveHeader("tbl_name", hdr);

        ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(ddb).putItem(captor.capture());

        PutItemRequest req = captor.getValue();
        assertEquals("tbl_name", req.tableName());

        Map<String, AttributeValue> item = req.item();
        assertEquals("H1", item.get("id").s());
        assertEquals("file.csv", item.get("file_name").s());
        assertEquals("customer", item.get("domain_name").s());
        assertEquals("ORG1", item.get("organization_id").s());
        assertEquals("POL1", item.get("policy_id").s());
        assertEquals("user@example.com", item.get("uploaded_by").s());
        assertEquals("5", item.get("total_rows_count").n());
        assertEquals(FileProcessStage.UNPROCESSED.toString(), item.get("process_stage").s());
        assertEquals("NEW", item.get("file_status").s());

        String uploaded = item.get("uploaded_date").s();
        assertTrue(Pattern.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", uploaded));
       
        assertEquals("", item.get("updated_date").s());
    }

    @Test
    void fetchOldestByStage_returnsEarliestUploadedDate() {
     
        Map<String, AttributeValue> it1 = item("A1", "2025-01-02 01:00:00", "dom", "org", "pol", "u@e.com", 10);
        Map<String, AttributeValue> it2 = item("A2", "2025-01-01 23:00:00", "dom", "org", "pol", "u@e.com", 11);

        ScanResponse page = ScanResponse.builder().items(it1, it2).build();

        // mock paginator
        ScanIterable iterable = mock(ScanIterable.class);
        when(ddb.scanPaginator(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenReturn(iterable);
        when(iterable.iterator()).thenReturn(List.of(page).iterator());

        var out = service.fetchOldestByStage(FileProcessStage.PROCESSING);
        assertTrue(out.isPresent());
        assertEquals("A2", out.get().getId());
        assertEquals("dom", out.get().getDomainName());
        assertEquals("org", out.get().getOrganizationId());
        assertEquals("pol", out.get().getPolicyId());
        assertEquals("u@e.com", out.get().getUploadedBy());
        assertEquals(11, out.get().getTotalRowsCount());
    }

    @Test
    void fetchOldestByStage_noItems_returnsEmpty() {
        ScanResponse empty = ScanResponse.builder().items(Collections.emptyList()).build();
        ScanIterable iterable = mock(ScanIterable.class);
        when(ddb.scanPaginator(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenReturn(iterable);
        when(iterable.iterator()).thenReturn(List.of(empty).iterator());

        var out = service.fetchOldestByStage(FileProcessStage.UNPROCESSED);
        assertTrue(out.isEmpty());
    }

   

    @Test
    void updateFileStage_buildsCorrectUpdateRequest() {
        service.updateFileStage("F1", FileProcessStage.PROCESSING);

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(ddb).updateItem(captor.capture());

        UpdateItemRequest req = captor.getValue();
        assertEquals("md_header", req.tableName());
        assertEquals("F1", req.key().get("id").s());
        assertEquals("attribute_exists(id)", req.conditionExpression());
        assertEquals(ReturnValue.UPDATED_NEW, req.returnValues());

        assertTrue(req.updateExpression().contains("#ps = :ps"));
        assertTrue(req.updateExpression().contains("updated_date = :now"));
        assertEquals("process_stage", req.expressionAttributeNames().get("#ps"));
        assertEquals(FileProcessStage.PROCESSING.name(), req.expressionAttributeValues().get(":ps").s());

        String now = req.expressionAttributeValues().get(":now").s();
        assertTrue(Pattern.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", now));
    }


    @Test
    void filenameExists_blank_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> service.filenameExists("  "));
        assertThrows(IllegalArgumentException.class, () -> service.filenameExists(null));
    }

    @Test
    void filenameExists_countPositive_returnsTrue() {
        ScanResponse page = ScanResponse.builder().count(2).build();

        ScanIterable iterable = mock(ScanIterable.class);
        when(ddb.scanPaginator(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenReturn(iterable);
        when(iterable.iterator()).thenReturn(List.of(page).iterator());

        assertTrue(service.filenameExists("data.csv"));
    }

    @Test
    void filenameExists_zeroCountAcrossPages_returnsFalse() {
        ScanResponse p1 = ScanResponse.builder().count(0).build();
        ScanResponse p2 = ScanResponse.builder().count(0).build();

        ScanIterable iterable = mock(ScanIterable.class);
        when(ddb.scanPaginator(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenReturn(iterable);
        when(iterable.iterator()).thenReturn(List.of(p1, p2).iterator());

        assertFalse(service.filenameExists("data.csv"));
    }

    @Test
    void filenameExists_scanThrows_rethrows() {
        
        var ex = software.amazon.awssdk.services.dynamodb.model.DynamoDbException.builder()
                .message("boom")
                .build();
        when(ddb.scanPaginator(any(software.amazon.awssdk.services.dynamodb.model.ScanRequest.class)))
        .thenThrow(ex);

        var thrown = assertThrows(
                software.amazon.awssdk.services.dynamodb.model.DynamoDbException.class,
                () -> service.filenameExists("data.csv"));
        assertEquals("boom", thrown.getMessage());
    }

   

    private static Map<String, AttributeValue> item(
            String id, String uploadedDate, String domain, String org, String pol, String uploadedBy, int totalRows) {

        Map<String, AttributeValue> m = new LinkedHashMap<>();
        m.put("id", AttributeValue.builder().s(id).build());
        m.put("uploaded_date", AttributeValue.builder().s(uploadedDate).build());
        m.put("domain_name", AttributeValue.builder().s(domain).build());
        m.put("organization_id", AttributeValue.builder().s(org).build());
        m.put("policy_id", AttributeValue.builder().s(pol).build());
        m.put("uploaded_by", AttributeValue.builder().s(uploadedBy).build());
        m.put("total_rows_count", AttributeValue.builder().n(String.valueOf(totalRows)).build());
        return m;
    }
}
