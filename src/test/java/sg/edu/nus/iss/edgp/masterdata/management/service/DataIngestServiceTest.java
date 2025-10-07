package sg.edu.nus.iss.edgp.masterdata.management.service;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.masterdata.management.dto.InsertionSummary;
import sg.edu.nus.iss.edgp.masterdata.management.dto.UploadResult;
import sg.edu.nus.iss.edgp.masterdata.management.jwt.JWTService;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DataIngestService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.StagingDataService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

@ExtendWith(MockitoExtension.class)
public class DataIngestServiceTest {

 private DynamoDbClient dynamoDbClient;
 private JWTService jwtService;
 private DynamicDetailService dynamoService;
 private HeaderService headerService;
 private StagingDataService stagingDataService;
 private MasterdataService masterdataService;

 private DataIngestService service;

 @BeforeEach
 void setUp() {
     dynamoDbClient = mock(DynamoDbClient.class);
     jwtService = mock(JWTService.class);
     dynamoService = mock(DynamicDetailService.class);
     headerService = mock(HeaderService.class);
     stagingDataService = mock(StagingDataService.class);
     masterdataService = mock(MasterdataService.class);

     service = new DataIngestService(
             dynamoDbClient, jwtService, dynamoService, headerService, stagingDataService, masterdataService
     );

     // Inject @Value fields
     ReflectionTestUtils.setField(service, "headerTableName", "md_header");
     ReflectionTestUtils.setField(service, "stagingTableName", "md_staging");
     ReflectionTestUtils.setField(service, "mdataTaskTrackerTable", "md_task_tracker");
 }

 // ---------------- processIngest: validation & error paths ----------------

 @Test
 void processIngest_nullRequest_returnsErrorUploadResult() {
     UploadResult res = service.processIngest(null, "Bearer abc");
     assertEquals("Request body is required.", res.getMessage());
     assertEquals(0, res.getTotalRecord());
 }

 @Test
 void processIngest_missingDataKeyType_returnsError() {
     Map<String, Object> req = Map.of("data", "not-an-object");
     UploadResult res = service.processIngest(req, "Bearer abc");
     assertEquals("'data' must be an object.", res.getMessage());
 }

 @Test
 void processIngest_missingDomain_returnsError() {
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("policy_id", "P1");
     data.put("col1", "v");
     Map<String, Object> req = Map.of("data", data);

     UploadResult res = service.processIngest(req, "Bearer abc");
     assertEquals("domain_name is mandatory.", res.getMessage());
 }

 @Test
 void processIngest_missingPolicy_returnsError() {
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("domain_name", "customer");
     data.put("col1", "v");
     Map<String, Object> req = Map.of("data", data);

     UploadResult res = service.processIngest(req, "Bearer abc");
     assertEquals("policy_id is mandatory.", res.getMessage());
 }

 @Test
 void processIngest_noRowFields_returnsError() {
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("domain_name", "customer");
     data.put("policy_id", "P1");
     data.put("uploaded_by", "alice@x.com");
     Map<String, Object> req = Map.of("data", data);

     UploadResult res = service.processIngest(req, "Bearer abc");
     assertEquals("No row fields provided in data.", res.getMessage());
 }

 // ---------------- processIngest: happy paths & behavior ----------------

 @Test
 void processIngest_uploadedByPayload_overridesJwt_andHeaderCaptured() {
     // uploaded_by provided -> service should NOT call jwtService.extractUserEmailFromToken(...)
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("domain_name", "customer");
     data.put("policy_id", "P1");
     data.put("uploaded_by", "payload@x.com");
     data.put("name", "Alice");
     Map<String, Object> req = Map.of("data", data);

     // By default, mock boolean returns false, so tables will be created; it's fine.
     // Stub staging insert to succeed
     when(stagingDataService.insertToStaging(
             anyString(), anyList(), anyString(), anyString(), anyString(), anyString(), anyString()
     )).thenReturn(new InsertionSummary(1, List.of(Map.of("name", "Alice"))));

     // Capture saved header
     ArgumentCaptor<MasterDataHeader> headerCap = ArgumentCaptor.forClass(MasterDataHeader.class);

     UploadResult res = service.processIngest(req, "Bearer token");
     assertEquals("Data created successfully.", res.getMessage());

     verify(headerService).saveHeader(eq("md_header"), headerCap.capture());
     MasterDataHeader saved = headerCap.getValue();
     assertEquals("payload@x.com", saved.getUploadedBy());
     assertEquals("customer", saved.getDomainName());
     assertEquals("P1", saved.getPolicyId());
     assertNotNull(saved.getId());
     assertDoesNotThrow(() -> Instant.parse(saved.getUploadDate()));
 }

 @Test
 @SuppressWarnings("unchecked")
 void processIngest_missingTables_areCreated_andReservedKeysFiltered_withUploadedByInPayload() {
     // uploaded_by provided -> only orgId comes from JWT
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("domain_name", "customer");
     data.put("policy_id", "P1");
     data.put("uploaded_by", "alice@issuer.com"); // should NOT go into row
     data.put("name", "Alice");
     Map<String, Object> req = Map.of("data", data);

     when(jwtService.extractOrgIdFromToken("jwt")).thenReturn("ORG-1"); // called even if uploaded_by is present

     // tables missing -> create both
     when(dynamoService.tableExists("md_header")).thenReturn(false);
     when(dynamoService.tableExists("md_staging")).thenReturn(false);

     ArgumentCaptor<List<LinkedHashMap<String,Object>>> rowsCap = ArgumentCaptor.forClass(List.class);
     when(stagingDataService.insertToStaging(
             anyString(), rowsCap.capture(), anyString(), anyString(), anyString(), anyString(), anyString()
     )).thenReturn(new InsertionSummary(1, List.of(Map.of("name", "Alice"))));

     UploadResult res = service.processIngest(req, "Bearer jwt");
     assertEquals("Data created successfully.", res.getMessage());

     // created once each
     verify(dynamoService).createTable("md_header");
     verify(dynamoService).createTable("md_staging");

     // reserved keys filtered
     List<LinkedHashMap<String,Object>> rows = rowsCap.getValue();
     assertEquals(1, rows.size());
     Map<String,Object> row = rows.get(0);
     assertEquals(1, row.size());
     assertEquals("Alice", row.get("name"));
     assertFalse(row.containsKey("uploaded_by"));
     assertFalse(row.containsKey("domain_name"));
     assertFalse(row.containsKey("policy_id"));
 }

 @Test
 void processIngest_noUploadedBy_usesJwtEmail_andOrgId() {
     // uploaded_by NOT provided -> service should use JWT email + orgId
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("domain_name", "customer");
     data.put("policy_id", "P1");
     data.put("name", "Alice");
     Map<String, Object> req = Map.of("data", data);

     when(dynamoService.tableExists(anyString())).thenReturn(true); // skip create branch
     when(jwtService.extractUserEmailFromToken("jwt")).thenReturn("jwt@issuer.com");
     when(jwtService.extractOrgIdFromToken("jwt")).thenReturn("ORG-9");

     when(stagingDataService.insertToStaging(
             anyString(), anyList(), anyString(), anyString(), anyString(), anyString(), anyString()
     )).thenReturn(new InsertionSummary(1, List.of(Map.of("name", "Alice"))));

     // Capture header to ensure uploadedBy is from JWT
     ArgumentCaptor<MasterDataHeader> headerCap = ArgumentCaptor.forClass(MasterDataHeader.class);

     UploadResult res = service.processIngest(req, "Bearer jwt");
     assertEquals("Data created successfully.", res.getMessage());

     verify(headerService).saveHeader(eq("md_header"), headerCap.capture());
     MasterDataHeader saved = headerCap.getValue();
     assertEquals("jwt@issuer.com", saved.getUploadedBy());
     assertEquals("customer", saved.getDomainName());
     assertEquals("P1", saved.getPolicyId());
 }

 @Test
 void processIngest_conditionalFailure_isRethrown() {
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("domain_name", "customer");
     data.put("policy_id", "P1");
     data.put("name", "Alice");
     Map<String, Object> req = Map.of("data", data);

     when(dynamoService.tableExists(anyString())).thenReturn(true);

     ConditionalCheckFailedException ccfe =
             (ConditionalCheckFailedException) ConditionalCheckFailedException.builder().build();

     when(stagingDataService.insertToStaging(
             anyString(), anyList(), anyString(), anyString(), anyString(), anyString(), anyString()
     )).thenThrow(ccfe);

     assertThrows(ConditionalCheckFailedException.class, () -> service.processIngest(req, "Bearer t"));
 }

 @Test
 void processIngest_unhandledException_returnsGenericFailure() {
     Map<String, Object> data = new LinkedHashMap<>();
     data.put("domain_name", "customer");
     data.put("policy_id", "P1");
     data.put("name", "Alice");
     Map<String, Object> req = Map.of("data", data);

     when(dynamoService.tableExists(anyString())).thenReturn(true);
     // force header save to fail
     doThrow(new RuntimeException("boom"))
             .when(headerService).saveHeader(anyString(), any(MasterDataHeader.class));

     UploadResult res = service.processIngest(req, "Bearer t");
     assertEquals("Data create failed.", res.getMessage());
     assertEquals(0, res.getTotalRecord());
 }

 // ---------------- updateDataToTable ----------------

 @Test
 void updateDataToTable_success_populatesJwtFields_andForwards() {
     Map<String, Object> payload = new HashMap<>();
     payload.put("k", "v");

     when(jwtService.extractUserEmailFromToken("tok")).thenReturn("bob@x.com");
     when(jwtService.extractOrgIdFromToken("tok")).thenReturn("ORG-9");

     UploadResult expected = new UploadResult("ok", 1, List.of(Map.of("k", "v")));
     when(masterdataService.updateDataToTable(anyMap())).thenReturn(expected);

     UploadResult res = service.updateDataToTable(payload, "Bearer tok");
     assertNotNull(res);
     assertEquals("ok", res.getMessage());

     ArgumentCaptor<Map<String, Object>> cap = ArgumentCaptor.forClass(Map.class);
     verify(masterdataService).updateDataToTable(cap.capture());
     Map<String, Object> forwarded = cap.getValue();
     assertEquals("ORG-9", forwarded.get("organization_id"));
     assertEquals("bob@x.com", forwarded.get("uploaded_by"));
     assertEquals("v", forwarded.get("k"));
 }

 @Test
 void updateDataToTable_exception_returnsNull() {
     when(jwtService.extractUserEmailFromToken(any())).thenThrow(new RuntimeException("x"));
     UploadResult res = service.updateDataToTable(new HashMap<>(), "Bearer abc");
     assertNull(res);
 }

 @Test
 void updateDataToTable_headerWithoutBearerPrefix_stillWorks() {
     when(jwtService.extractUserEmailFromToken("raw")).thenReturn("eve@x.com");
     when(jwtService.extractOrgIdFromToken("raw")).thenReturn("ORG-1");
     when(masterdataService.updateDataToTable(anyMap()))
             .thenReturn(new UploadResult("ok", 1, List.of()));

     UploadResult res = service.updateDataToTable(new HashMap<>(), "raw");
     assertEquals("ok", res.getMessage());
 }
}

