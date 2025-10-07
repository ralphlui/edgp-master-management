package sg.edu.nus.iss.edgp.masterdata.management.service;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*; 

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import sg.edu.nus.iss.edgp.masterdata.management.aws.service.SQSPublishingService;
import sg.edu.nus.iss.edgp.masterdata.management.dto.InsertionSummary;
import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyData;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.PayloadBuilderService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.StagingDataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVParser;
import sg.edu.nus.iss.edgp.masterdata.management.utility.GeneralUtility;
import sg.edu.nus.iss.edgp.masterdata.management.utility.JSONReader;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

@ExtendWith(MockitoExtension.class)
public class MasterdataServiceTest {

    private DynamoDbClient dynamoDbClient;
    private JWTService jwtService;
    private DynamicDetailService dynamoService;
    private HeaderService headerService;
    private SQSPublishingService sqsPublishingService;
    private StagingDataService stagingDataService;
    private PayloadBuilderService payloadBuilderService;
    private JSONReader jsonReader;
    private GeneralUtility generalUtility;

    private MasterdataService svc;

    @BeforeEach
    void setUp() {
        dynamoDbClient = mock(DynamoDbClient.class);
        jwtService = mock(JWTService.class);
        dynamoService = mock(DynamicDetailService.class);
        headerService = mock(HeaderService.class);
        sqsPublishingService = mock(SQSPublishingService.class);
        stagingDataService = mock(StagingDataService.class);
        payloadBuilderService = mock(PayloadBuilderService.class);
        jsonReader = mock(JSONReader.class);
        generalUtility = mock(GeneralUtility.class);

        svc = new MasterdataService(
                dynamoDbClient,
                jwtService,
                dynamoService,
                headerService,
                sqsPublishingService,
                stagingDataService,
                payloadBuilderService,
                jsonReader,
                generalUtility
        );

        // Inject @Value fields
        ReflectionTestUtils.setField(svc, "headerTableName", "md_header");
        ReflectionTestUtils.setField(svc, "stagingTableName", "md_staging");
        ReflectionTestUtils.setField(svc, "mdataTaskTrackerTable", "md_tracker");
    }

    @Test
    void uploadCsvDataToTable_emptyCsv_returnsMessage() throws Exception {
        MultipartFile file = mock(MultipartFile.class);
        UploadRequest req = mock(UploadRequest.class);

        try (MockedStatic<CSVParser> csv = mockStatic(CSVParser.class)) {
            csv.when(() -> CSVParser.parseCsvObjects(file)).thenReturn(Collections.emptyList());

            UploadResult res = svc.uploadCsvDataToTable(file, req, "Bearer tok");

            assertEquals("CSV is empty.", res.getMessage());
            assertEquals(0, res.getTotalRecord());
            

            verifyNoInteractions(jwtService, dynamoService, headerService, stagingDataService, sqsPublishingService, dynamoDbClient);
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    void uploadCsvDataToTable_happyPath_createsTables_savesHeader_insertsRows() throws Exception {
        MultipartFile file = mock(MultipartFile.class);
        when(file.getOriginalFilename()).thenReturn("data.csv");

        UploadRequest req = mock(UploadRequest.class);
        when(req.getDomainName()).thenReturn("customer");
        when(req.getPolicyId()).thenReturn("POL1");

        Map<String, Object> r1 = new LinkedHashMap<>();
        r1.put("name", "Alice");
        Map<String, Object> r2 = new LinkedHashMap<>();
        r2.put("name", "Bob");

        try (MockedStatic<CSVParser> csv = mockStatic(CSVParser.class)) {
            csv.when(() -> CSVParser.parseCsvObjects(file)).thenReturn(List.of(r1, r2));

            when(jwtService.extractUserEmailFromToken("tok")).thenReturn("u@x.com");
            when(jwtService.extractOrgIdFromToken("tok")).thenReturn("ORG1");

            when(dynamoService.tableExists("md_header")).thenReturn(false);
            when(dynamoService.tableExists("md_staging")).thenReturn(false);

            ArgumentCaptor<List<LinkedHashMap<String, Object>>> rowsCap = ArgumentCaptor.forClass(List.class);
            when(stagingDataService.insertToStaging(
                    eq("md_staging"),
                    rowsCap.capture(),
                    eq("ORG1"),
                    eq("POL1"),
                    eq("customer"),
                    anyString(),
                    eq("u@x.com")
            )).thenReturn(new InsertionSummary(2, List.of(r1, r2)));

            UploadResult res = svc.uploadCsvDataToTable(file, req, "Bearer tok");

            assertEquals("Uploaded 2 rows successfully.", res.getMessage());
            assertEquals(2, res.getTotalRecord());
           

            verify(dynamoService).createTable("md_header");
            verify(dynamoService).createTable("md_staging");
            verify(headerService).saveHeader(eq("md_header"), any(MasterDataHeader.class));

            List<LinkedHashMap<String, Object>> rowsInserted = rowsCap.getValue();
            assertEquals(2, rowsInserted.size());
            assertEquals("Alice", rowsInserted.get(0).get("name"));
            assertEquals("Bob", rowsInserted.get(1).get("name"));
        }
    }

   

    @Test
    void getDataByPolicyAndDomainName_happy_scanAndMap() {
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
        when(jwtService.extractUserEmailFromToken("tok")).thenReturn("u@x.com");

        
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("ID1").build());
        item.put("policy_id", AttributeValue.builder().s("POL1").build());
        item.put("domain_name", AttributeValue.builder().s("customer").build());
        item.put("uploaded_by", AttributeValue.builder().s("u@x.com").build());
        item.put("organization_id", AttributeValue.builder().s("ORG1").build());
        item.put("process_stage", AttributeValue.builder().s(FileProcessStage.UNPROCESSED.toString()).build());
       
        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(item).build());

       
        PolicyData pdata = mock(PolicyData.class);
        when(pdata.getPolicyName()).thenReturn("Customer Policy");
        PolicyRoot proot = mock(PolicyRoot.class);
        when(proot.getData()).thenReturn(pdata);
        when(jsonReader.getValidationRules(eq("POL1"), eq("Bearer tok"))).thenReturn(proot);

        when(jsonReader.getOrganizationName(eq("ORG1"), eq("Bearer tok"))).thenReturn("Acme Org");

        SearchRequest req = new SearchRequest();
        req.setPolicyId("POL1");
        req.setDomainName("customer");

        List<Map<String, Object>> out = svc.getDataByPolicyAndDomainName(req, "Bearer tok");
        assertEquals(1, out.size());

        Map<String, Object> row = out.get(0);
        // basic fields
        assertEquals("ID1", row.get("id"));
        assertEquals("customer", row.get("domain_name"));
        // enriched organization
        Map<?, ?> org = (Map<?, ?>) row.get("organization");
        assertEquals("ORG1", org.get("id"));
        assertEquals("Acme Org", org.get("name"));
        // enriched policy
        Map<?, ?> policy = (Map<?, ?>) row.get("policy");
        assertEquals("POL1", policy.get("id"));
        assertEquals("Customer Policy", policy.get("name"));
    }

    @Test
    void getAllData_tableMissing_returnsEmpty() {
        when(jwtService.extractUserEmailFromToken("tok")).thenReturn("u@x.com");
        when(dynamoService.tableExists("md_staging")).thenReturn(false);

        List<Map<String, Object>> out = svc.getAllData("Bearer tok");
        assertTrue(out.isEmpty());
    }


    @Test
    void getAllUploadFiles_happy_scanHeaders() {
        when(jwtService.extractUserEmailFromToken("tok")).thenReturn("u@x.com");
        when(dynamoService.tableExists("md_header")).thenReturn(true);

        Map<String, AttributeValue> header = new LinkedHashMap<>();
        header.put("id", AttributeValue.builder().s("H1").build());
        header.put("uploaded_by", AttributeValue.builder().s("u@x.com").build());
        header.put("file_name", AttributeValue.builder().s("data.csv").build());
        header.put("process_stage", AttributeValue.builder().s(FileProcessStage.UNPROCESSED.toString()).build());

        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(header).build());

        List<Map<String, Object>> out = svc.getAllUploadFiles("Bearer tok");
        assertEquals(1, out.size());
        assertEquals("H1", out.get(0).get("id"));
        assertEquals("data.csv", out.get(0).get("file_name"));
    }


    @Test
    void processAndSendRawDataToSqs_noHeader_returnsZero() {
        when(headerService.fetchOldestByStage(FileProcessStage.UNPROCESSED))
                .thenReturn(Optional.empty());

        int processed = svc.processAndSendRawDataToSqs();
        assertEquals(0, processed);
        verifyNoInteractions(sqsPublishingService);
    }

    @Test
    void processAndSendRawDataToSqs_happyPath_sendsAllAndUpdates_noUnnecessaryStubs() throws Exception {
        // Header: all required fields present
        MasterDataHeader hdr = new MasterDataHeader();
        hdr.setId("F-1");
        hdr.setPolicyId("POLX");
        hdr.setDomainName("customer");
        hdr.setUploadedBy("user@x.com");
        hdr.setOrganizationId("ORG9");
        hdr.setTotalRowsCount(2);
        when(headerService.fetchOldestByStage(FileProcessStage.UNPROCESSED))
                .thenReturn(Optional.of(hdr));

       
        Map<String, AttributeValue> rec1 = new LinkedHashMap<>();
        rec1.put("id", AttributeValue.builder().s("S1").build());
        Map<String, AttributeValue> rec2 = new LinkedHashMap<>();
        rec2.put("id", AttributeValue.builder().s("S2").build());

        when(dynamoService.getUnprocessedRecordsByFileId(
                anyString(), eq("F-1"), eq("POLX"), eq("customer")))
            .thenReturn(List.of(rec1, rec2));

         
        when(dynamoService.tableExists(anyString())).thenReturn(true);
 
        when(dynamoService.claimStagingRow(anyString(), eq("S1"))).thenReturn(true);
        when(dynamoService.claimStagingRow(anyString(), eq("S2"))).thenReturn(true);

        
        when(jsonReader.getAccessToken("user@x.com")).thenReturn("atoken");

        PolicyRoot proot = mock(PolicyRoot.class);
        PolicyData pdata = mock(PolicyData.class);
        when(proot.getData()).thenReturn(pdata);
        when(pdata.getRules()).thenReturn(Collections.emptyList()); // only what's used

        when(jsonReader.getValidationRules("POLX", "Bearer atoken")).thenReturn(proot);
        when(payloadBuilderService.build(any(), anyMap(), anyList())).thenReturn("{json}");

        
        int processed = svc.processAndSendRawDataToSqs();

        assertEquals(2, processed);
 
        verify(sqsPublishingService, times(2)).sendRecordToQueue("{json}");
        verify(dynamoService, times(2)).insertValidatedMasterData(anyString(), anyMap());
        verify(headerService, atLeastOnce()).updateFileStage("F-1", FileProcessStage.PROCESSING);
        verify(dynamoService).markProcessed(anyString(), eq("S1"));
        verify(dynamoService).markProcessed(anyString(), eq("S2"));
        verify(dynamoService).updateStagingProcessedStatus(anyString(), eq("F-1"), eq("1"));

        
        verify(dynamoService, never()).createTable(anyString());
    }


 

    @Test
    void updateDataToTable_nullRequest_returnsErrorUploadResult() {
        UploadResult res = svc.updateDataToTable(null);
        assertEquals("Request body is required.", res.getMessage());
        assertEquals(0, res.getTotalRecord());
    }

    @Test
    void updateDataToTable_missingStagingItem_returnsMessage() {
        Map<String, Object> data = new HashMap<>();
        data.put("id", "STG-1");
        Map<String, Object> req = new HashMap<>();
        req.put("data", data);

       
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(Collections.emptyMap()).build());

        UploadResult res = svc.updateDataToTable(req);
        assertEquals("Staging item not found for Id: STG-1", res.getMessage());
        assertEquals(0, res.getTotalRecord());
    }


    @Test
    void getDataByPolicyId_happy_scanAndMap() {
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
        when(jwtService.extractUserEmailFromToken("tok")).thenReturn("u@x.com");

        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("ID1").build());
        item.put("policy_id", AttributeValue.builder().s("POL1").build());
        item.put("uploaded_by", AttributeValue.builder().s("u@x.com").build());
        item.put("organization_id", AttributeValue.builder().s("ORG1").build());

       
        item.put("process_stage",
            AttributeValue.builder().s(FileProcessStage.PROCESSING.toString()).build());

        when(dynamoDbClient.scan(any(ScanRequest.class)))
            .thenReturn(ScanResponse.builder().items(item).build());

        PolicyData pdata = mock(PolicyData.class);
        when(pdata.getPolicyName()).thenReturn("Customer Policy");
        PolicyRoot proot = mock(PolicyRoot.class);
        when(proot.getData()).thenReturn(pdata);
        when(jsonReader.getValidationRules(eq("POL1"), eq("Bearer tok"))).thenReturn(proot);
        when(jsonReader.getOrganizationName(eq("ORG1"), eq("Bearer tok"))).thenReturn("Acme Org");

        SearchRequest req = new SearchRequest();
        req.setPolicyId("POL1");

        List<Map<String, Object>> out = svc.getDataByPolicyId(req, "Bearer tok");
        Map<String, Object> row = out.get(0);

        assertEquals(FileProcessStage.PROCESSING.toString(), row.get("process_stage"));
    }


    @Test
    void getDataByDomainName_happy_scanAndMap() {
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
        when(jwtService.extractUserEmailFromToken("tok")).thenReturn("u@x.com");

        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("IDD").build());
        item.put("domain_name", AttributeValue.builder().s("supplier").build());
        item.put("uploaded_by", AttributeValue.builder().s("u@x.com").build());
        item.put("policy_id", AttributeValue.builder().s("POL-D").build());
        item.put("organization_id", AttributeValue.builder().s("ORG-D").build());
        item.put("process_stage", AttributeValue.builder().s(FileProcessStage.UNPROCESSED.toString()).build());

        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(item).build());

        // enrich
        PolicyRoot pr = mock(PolicyRoot.class);
        PolicyData pd = mock(PolicyData.class);
        when(pr.getData()).thenReturn(pd);
        when(pd.getPolicyName()).thenReturn("Domain Policy");
        when(jsonReader.getValidationRules("POL-D", "Bearer tok")).thenReturn(pr);
        when(jsonReader.getOrganizationName("ORG-D", "Bearer tok")).thenReturn("Org D");

        SearchRequest req = new SearchRequest();
        req.setDomainName("supplier");

        List<Map<String, Object>> out = svc.getDataByDomainName(req, "Bearer tok");
        assertEquals(1, out.size());
        Map<String, Object> row = out.get(0);
        assertEquals("IDD", row.get("id"));
        assertEquals("supplier", row.get("domain_name"));
        assertEquals("Domain Policy", ((Map<?,?>) row.get("policy")).get("name"));
        assertEquals("Org D", ((Map<?,?>) row.get("organization")).get("name"));
    }

    @Test
    void getDataByFileId_happy_scanAndMap() {
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
        when(jwtService.extractUserEmailFromToken("tok")).thenReturn("u@x.com");

        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("IDF").build());
        item.put("file_id", AttributeValue.builder().s("FILE-9").build());
        item.put("uploaded_by", AttributeValue.builder().s("u@x.com").build());
        item.put("policy_id", AttributeValue.builder().s("POL-F").build());
        item.put("organization_id", AttributeValue.builder().s("ORG-F").build());
        item.put("process_stage", AttributeValue.builder().s(FileProcessStage.PROCESSING.toString()).build());

        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(item).build());

        // enrich
        PolicyRoot pr = mock(PolicyRoot.class);
        PolicyData pd = mock(PolicyData.class);
        when(pr.getData()).thenReturn(pd);
        when(pd.getPolicyName()).thenReturn("File Policy");
        when(jsonReader.getValidationRules("POL-F", "Bearer tok")).thenReturn(pr);
        when(jsonReader.getOrganizationName("ORG-F", "Bearer tok")).thenReturn("Org F");

        SearchRequest req = new SearchRequest();
        req.setFileId("FILE-9");

        List<Map<String, Object>> out = svc.getDataByFileId(req, "Bearer tok");
        assertEquals(1, out.size());
        Map<String, Object> row = out.get(0);
        assertEquals("IDF", row.get("id"));
        assertEquals("File Policy", ((Map<?,?>) row.get("policy")).get("name"));
        assertEquals("Org F", ((Map<?,?>) row.get("organization")).get("name"));
    }

    @Test
    void processAndSendRawDataToSqs_headerMissingFields_throws() {
        MasterDataHeader bad = new MasterDataHeader();
        bad.setId("F1");
        bad.setPolicyId("POL");
        bad.setDomainName("customer");
        bad.setUploadedBy("");
        bad.setOrganizationId("ORG");
        bad.setTotalRowsCount(1);

        when(headerService.fetchOldestByStage(FileProcessStage.UNPROCESSED))
                .thenReturn(Optional.of(bad));

        MasterdataServiceException ex = assertThrows(
                MasterdataServiceException.class,
                () -> svc.processAndSendRawDataToSqs());
        assertTrue(ex.getMessage().startsWith("Process and send data to SQS failed: Missing header info"));
    }


    @Test
    void updateDataToTable_noChanges_returnsNoChangesMessage() {
       
        MasterdataService realSvc = new MasterdataService(
                dynamoDbClient, jwtService, dynamoService, headerService,
                sqsPublishingService, stagingDataService, payloadBuilderService, jsonReader,
                new GeneralUtility()
        );
        ReflectionTestUtils.setField(realSvc, "headerTableName", "md_header");
        ReflectionTestUtils.setField(realSvc, "stagingTableName", "md_staging");
        ReflectionTestUtils.setField(realSvc, "mdataTaskTrackerTable", "md_tracker");

        Map<String, AttributeValue> stg = new LinkedHashMap<>();
        stg.put("id", AttributeValue.builder().s("STG-1").build());
        stg.put("file_id", AttributeValue.builder().s("F-1").build());

        Map<String, AttributeValue> hdr = new LinkedHashMap<>();
        hdr.put("id", AttributeValue.builder().s("F-1").build());
        hdr.put("file_name", AttributeValue.builder().s("data.csv").build());

        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(stg).build())
                .thenReturn(GetItemResponse.builder().item(hdr).build());

        Map<String, Object> data = Map.of("id", "STG-1");
        Map<String, Object> req = Map.of("data", data);

        UploadResult res = realSvc.updateDataToTable(req);
        assertEquals("No changes applied (both header & staging identical).", res.getMessage());
        assertEquals(0, res.getTotalRecord());
        assertEquals(2, res.getData().size());
    }

    @Test
    void updateDataToTable_happy_updatesStagingOnly() {
        MasterdataService realSvc = new MasterdataService(
                dynamoDbClient, jwtService, dynamoService, headerService,
                sqsPublishingService, stagingDataService, payloadBuilderService, jsonReader,
                new GeneralUtility()
        );
        ReflectionTestUtils.setField(realSvc, "headerTableName", "md_header");
        ReflectionTestUtils.setField(realSvc, "stagingTableName", "md_staging");
        ReflectionTestUtils.setField(realSvc, "mdataTaskTrackerTable", "md_tracker");

        Map<String, AttributeValue> stg = new LinkedHashMap<>();
        stg.put("id", AttributeValue.builder().s("STG-1").build());
        stg.put("file_id", AttributeValue.builder().s("F-1").build());
        stg.put("col1", AttributeValue.builder().s("old").build());

        Map<String, AttributeValue> hdr = new LinkedHashMap<>();
        hdr.put("id", AttributeValue.builder().s("F-1").build());
        hdr.put("file_name", AttributeValue.builder().s("data.csv").build());

        Map<String, AttributeValue> stgAfter = new LinkedHashMap<>(stg);
        stgAfter.put("col1", AttributeValue.builder().s("new").build());

       
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(stg).build())
                .thenReturn(GetItemResponse.builder().item(hdr).build())
                .thenReturn(GetItemResponse.builder().item(stgAfter).build())
                .thenReturn(GetItemResponse.builder().item(hdr).build());

        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().attributes(stgAfter).build());

        Map<String, Object> data = new HashMap<>();
        data.put("id", "STG-1");
        data.put("col1", "new");
        Map<String, Object> req = Map.of("data", data);

        UploadResult res = realSvc.updateDataToTable(req);

        assertEquals("Data updated successfully.", res.getMessage());
        assertEquals(1, res.getTotalRecord());
        assertEquals("new", ((Map<?, ?>) res.getData().get(0)).get("col1"));

       
        verify(dynamoDbClient, times(1)).updateItem(any(UpdateItemRequest.class));
    }


    @Test
    void updateDataToTable_stagingUpdateFails_returnsError() {
        MasterdataService realSvc = new MasterdataService(
                dynamoDbClient, jwtService, dynamoService, headerService,
                sqsPublishingService, stagingDataService, payloadBuilderService, jsonReader,
                new GeneralUtility()
        );
        ReflectionTestUtils.setField(realSvc, "headerTableName", "md_header");
        ReflectionTestUtils.setField(realSvc, "stagingTableName", "md_staging");
        ReflectionTestUtils.setField(realSvc, "mdataTaskTrackerTable", "md_tracker");

        Map<String, AttributeValue> stg = new LinkedHashMap<>();
        stg.put("id", AttributeValue.builder().s("STG-1").build());
        stg.put("file_id", AttributeValue.builder().s("F-1").build());
        stg.put("col1", AttributeValue.builder().s("old").build());

        Map<String, AttributeValue> hdr = new LinkedHashMap<>();
        hdr.put("id", AttributeValue.builder().s("F-1").build());
        hdr.put("file_name", AttributeValue.builder().s("data.csv").build());

        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(stg).build())
                .thenReturn(GetItemResponse.builder().item(hdr).build());

     
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(new RuntimeException("boom"));

        Map<String, Object> data = new HashMap<>();
        data.put("id", "STG-1");
        data.put("col1", "new");
        Map<String, Object> req = Map.of("data", data);

        UploadResult res = realSvc.updateDataToTable(req);
        assertTrue(res.getMessage().startsWith("Failed to update staging:"));
       
        assertEquals(2, res.getData().size());
    }

}

