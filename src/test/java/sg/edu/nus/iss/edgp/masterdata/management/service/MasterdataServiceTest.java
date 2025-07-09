package sg.edu.nus.iss.edgp.masterdata.management.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.TemplateFileFormat;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.UploadRequest;
import sg.edu.nus.iss.edgp.masterdata.management.repository.MetadataRepository;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.CSVParser;

@ExtendWith(MockitoExtension.class)
class MasterdataServiceTest {

	@InjectMocks
	private MasterdataService masterdataService;

	@Mock
	private MetadataRepository metadataRepository;

	@Mock
	private JdbcTemplate jdbcTemplate;

	@Mock
	private CSVParser csvParser;

	@Mock
	private DataSource dataSource;

	@Mock
	private Connection connection;

	@BeforeEach
	void setup() throws Exception {
		lenient().when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
		lenient().when(dataSource.getConnection()).thenReturn(connection);
		lenient().when(connection.getCatalog()).thenReturn("test_schema");

		ReflectionTestUtils.setField(masterdataService, "jdbcTemplate", jdbcTemplate);
	}

	@Test
	void testUploadCsvDataToTable_success() throws Exception {
		MultipartFile mockFile = new MockMultipartFile("file", "test.csv", "text/csv", "id,name\n1,John".getBytes());

		UploadRequest req = new UploadRequest();
		req.setCategory("vendor");
		req.setOrganizationId("ORG123");
		req.setPolicyId("POLICY123");

		Map<String, String> row = new HashMap<>();
		row.put("name", "John");

		when(csvParser.parseCsv(any())).thenReturn(List.of(row));
		when(metadataRepository.tableExists("test_schema", "vendor")).thenReturn(true);

		String result = masterdataService.uploadCsvDataToTable(mockFile, req);

		assertEquals("Inserted 1 rows .", result);
		verify(metadataRepository).insertRow(eq("vendor"), any());
	}

	@Test
	void testUploadCsvDataToTable_tableDoesNotExist() throws IOException {
		MultipartFile mockFile = new MockMultipartFile("file", "test.csv", "text/csv", "id,name\n1,John".getBytes());

		UploadRequest req = new UploadRequest();
		req.setCategory("vendor");
		req.setOrganizationId("ORG123");
		req.setPolicyId("POLICY123");

		when(csvParser.parseCsv(any())).thenReturn(List.of(Map.of("name", "John")));
		when(metadataRepository.tableExists(any(), eq("vendor"))).thenReturn(false);

		Exception ex = assertThrows(MasterdataServiceException.class,
				() -> masterdataService.uploadCsvDataToTable(mockFile, req));

		assertEquals("No table found. Please set up the table before uploading data.", ex.getMessage());
	}

	@Test
	void testParseCsvTemplate_validFile() throws Exception {
		String content = "fieldName,description,dataType,length\nname,Name Field,STRING,100";
		MultipartFile file = new MockMultipartFile("file", "template.csv", "text/csv", content.getBytes());

		List<TemplateFileFormat> result = masterdataService.parseCsvTemplate(file);

		assertEquals(1, result.size());
		assertEquals("name", result.get(0).getFieldName());
		assertEquals("STRING", result.get(0).getDataType());
		assertEquals(100, result.get(0).getLength());
	}

	@Test
	void testGetAllData_returnsRecords() {
		SearchRequest req = new SearchRequest();
		req.setCategory("vendor");

		List<Map<String, Object>> mockResult = List.of(Map.of("id", 1, "name", "Test"));

		when(metadataRepository.getAllData(eq("vendor"), any(SearchRequest.class))).thenReturn(mockResult);

		List<Map<String, Object>> result = masterdataService.getAllData(req);

		assertEquals(1, result.size());
		assertEquals("Test", result.get(0).get("name"));
	}
	
	@Test
	void testCreateTableFromCsvTemplate_executesSql() throws Exception {
	 
	    String csvContent = "fieldName,description,dataType,length\nname,Name Field,STRING,100";
	    MultipartFile file = new MockMultipartFile("file", "template.csv", "text/csv", csvContent.getBytes());

	    MasterdataService spyService = Mockito.spy(masterdataService);
	    List<TemplateFileFormat> parsedFields = List.of(new TemplateFileFormat("name", "Name Field", "STRING", 100));

	    doReturn(parsedFields).when(spyService).parseCsvTemplate(file);

	    ReflectionTestUtils.setField(spyService, "jdbcTemplate", jdbcTemplate);

	    spyService.createTableFromCsvTemplate(file, "vendor");

	    
	}

	 @Test
	    void testGetDataByPolicyAndOrgId_returnsRecords() {
	    
	        SearchRequest request = new SearchRequest();
	        request.setCategory("vendor");
	        request.setOrganizationId("ORG001");
	        request.setPolicyId("POLICY001");

	        List<Map<String, Object>> mockData = List.of(
	                Map.of("id", "1", "name", "Sample Item")
	        );

	        when(metadataRepository.getDataByPolicyAndOrgId(eq("vendor"), eq(request)))
	                .thenReturn(mockData);

	        List<Map<String, Object>> result = masterdataService.getDataByPolicyAndOrgId(request);

	        assertEquals(1, result.size());
	        assertEquals("Sample Item", result.get(0).get("name"));
	        verify(metadataRepository).getDataByPolicyAndOrgId(eq("vendor"), eq(request));
	    }
	 
	 @Test
	 void testGetDataByPolicyId_returnsRecords() {
	    
	     SearchRequest request = new SearchRequest();
	     request.setCategory("vendor");
	     request.setPolicyId("POLICY001");

	     List<Map<String, Object>> mockData = List.of(
	         Map.of("id", "1", "name", "Item A")
	     );

	     when(metadataRepository.getDataByPolicyId(eq("vendor"), eq(request)))
	         .thenReturn(mockData);

	     List<Map<String, Object>> result = masterdataService.getDataByPolicyId(request);

	     assertEquals(1, result.size());
	     assertEquals("Item A", result.get(0).get("name"));
	     verify(metadataRepository).getDataByPolicyId(eq("vendor"), eq(request));
	 }
	 
	 @Test
	 void testGetDataByOrgId_returnsRecords() {
	 
	     SearchRequest request = new SearchRequest();
	     request.setCategory("vendor");
	     request.setOrganizationId("ORG001");

	     List<Map<String, Object>> mockData = List.of(
	         Map.of("id", "2", "name", "Item B")
	     );

	     when(metadataRepository.getDataByOrgId(eq("vendor"), eq(request)))
	         .thenReturn(mockData);

	     List<Map<String, Object>> result = masterdataService.getDataByOrgId(request);

	     assertEquals(1, result.size());
	     assertEquals("Item B", result.get(0).get("name"));
	     verify(metadataRepository).getDataByOrgId(eq("vendor"), eq(request));
	 }


	 @Test
	 void testGenerateTableName_validFilename() throws Exception {
	     Method method = MasterdataService.class.getDeclaredMethod("generateTableName", String.class);
	     method.setAccessible(true);

	     String result = (String) method.invoke(masterdataService, "Vendor-Data.csv");

	     assertEquals("mdm_vendor_data", result);
	 }
	 
	 @Test
	 void testCheckTableIfExists_returnsTrue() throws Exception {
	     when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
	     when(dataSource.getConnection()).thenReturn(connection);
	     when(connection.getCatalog()).thenReturn("test_schema");
	     when(metadataRepository.tableExists("test_schema", "vendor")).thenReturn(true);

	     boolean result = masterdataService.checkTableIfExists("vendor");

	     assertTrue(result);
	     verify(metadataRepository).tableExists("test_schema", "vendor");
	 }

	 @Test
	 void testCheckTableIfExists_returnsFalse() throws Exception {
	     when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
	     when(dataSource.getConnection()).thenReturn(connection);
	     when(connection.getCatalog()).thenReturn("test_schema");
	     when(metadataRepository.tableExists("test_schema", "vendor")).thenReturn(false);

	     boolean result = masterdataService.checkTableIfExists("vendor");

	     assertFalse(result);
	     verify(metadataRepository).tableExists("test_schema", "vendor");
	 }


}
