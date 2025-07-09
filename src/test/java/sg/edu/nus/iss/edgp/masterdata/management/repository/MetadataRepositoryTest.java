package sg.edu.nus.iss.edgp.masterdata.management.repository;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;

@ExtendWith(MockitoExtension.class)
class MetadataRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private MetadataRepository metadataRepository;
    
    @Mock
    private Connection connection;

    @Mock
    private DataSource dataSource;
    
    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData resultSetMetaData;


    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
         
        lenient().when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
		lenient().when(dataSource.getConnection()).thenReturn(connection);
		lenient().when(connection.getCatalog()).thenReturn("test_schema");
		
    }
    
    private SearchRequest buildSearchRequest() {
        SearchRequest req = new SearchRequest();
        req.setCategory("employee");
        req.setPolicyId("POL123");
        req.setOrganizationId("ORG123");
        req.setPage(0);
        req.setSize(10);
        return req;
    }

    @Test
    void testTableExists_returnsTrue() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(), any())).thenReturn(1);
        boolean result = metadataRepository.tableExists("schema", "table_name");
        assertTrue(result);
    }

    @Test
    void testTableExists_returnsFalse() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(), any())).thenReturn(0);
        boolean result = metadataRepository.tableExists("schema", "table_name");
        assertFalse(result);
    }

    @Test
    void testGetAllData() {
        SearchRequest req = new SearchRequest();
        req.setPage(0);
        req.setSize(10);
        List<Map<String, Object>> mockData = List.of(Map.of("id", 1, "name", "Test"));
        when(jdbcTemplate.queryForList(anyString(), eq(10), eq(0))).thenReturn(mockData);

        List<Map<String, Object>> result = metadataRepository.getAllData("table_name", req);
        assertEquals(1, result.size());
        assertEquals("Test", result.get(0).get("name"));
    }
    
    @Test
    void testValidateInsertColumns_validColumns_shouldPass() {
        String tableName = "test_table";
        Set<String> insertColumns = Set.of("name", "age");

        // Simulate columns retrieved from DB
        Set<String> dbColumns = new HashSet<>(Set.of("name", "age", "created_date", "updated_date"));

        // Mock jdbcTemplate.query
        when(jdbcTemplate.query(eq("SELECT * FROM `" + tableName + "` LIMIT 1"), any(ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                ResultSetExtractor<?> extractor = invocation.getArgument(1);
                ResultSet rs = mock(ResultSet.class);
                ResultSetMetaData meta = mock(ResultSetMetaData.class);

                when(meta.getColumnCount()).thenReturn(4);
                when(meta.getColumnName(1)).thenReturn("name");
                when(meta.getColumnName(2)).thenReturn("age");
                when(meta.getColumnName(3)).thenReturn("created_date");
                when(meta.getColumnName(4)).thenReturn("updated_date");
                when(rs.getMetaData()).thenReturn(meta);

                return ((ResultSetExtractor<?>) extractor).extractData(rs);
            });

        // No exception should be thrown
        assertDoesNotThrow(() -> metadataRepository.validateInsertColumns(tableName, insertColumns, jdbcTemplate));
    }

    @Test
    void testValidateInsertColumns_missingColumns_shouldThrow() {
        String tableName = "test_table";
        Set<String> insertColumns = Set.of("name", "invalid_col");

        // Simulate columns retrieved from DB
        Set<String> dbColumns = new HashSet<>(Set.of("name", "age", "created_date", "updated_date"));

        when(jdbcTemplate.query(eq("SELECT * FROM `" + tableName + "` LIMIT 1"), any(ResultSetExtractor.class)))
            .thenAnswer(invocation -> {
                ResultSetExtractor<?> extractor = invocation.getArgument(1);
                ResultSet rs = mock(ResultSet.class);
                ResultSetMetaData meta = mock(ResultSetMetaData.class);

                when(meta.getColumnCount()).thenReturn(4);
                when(meta.getColumnName(1)).thenReturn("name");
                when(meta.getColumnName(2)).thenReturn("age");
                when(meta.getColumnName(3)).thenReturn("created_date");
                when(meta.getColumnName(4)).thenReturn("updated_date");
                when(rs.getMetaData()).thenReturn(meta);

                return ((ResultSetExtractor<?>) extractor).extractData(rs);
            });

        Exception ex = assertThrows(IllegalArgumentException.class, () -> 
            metadataRepository.validateInsertColumns(tableName, insertColumns, jdbcTemplate)
        );

        assertTrue(ex.getMessage().contains("invalid_col"));
    }
    
    @Test
    void testNormalizeInsertData_validValues_shouldParseCorrectly() throws Exception {
        // Prepare mock column types
        Map<String, Integer> columnTypes = Map.of(
                "name", Types.VARCHAR,
                "age", Types.INTEGER,
                "salary", Types.DECIMAL,
                "birthdate", Types.DATE,
                "created_time", Types.TIMESTAMP
        );

        // Raw input data as string
        Map<String, String> rawData = Map.of(
                "name", "John Doe",
                "age", "30",
                "salary", "12345.67",
                "birthdate", "1990-01-01",
                "created_time", "2023-07-10 10:30:00"
        );

        // Inject getColumnTypes via reflection to avoid DB
        MetadataRepository spyRepo = Mockito.spy(metadataRepository);
        doReturn(columnTypes).when(spyRepo).getColumnTypes("employee");

        Map<String, Object> result = spyRepo.normalizeInsertData("employee", rawData, rawData.keySet());

        assertEquals("John Doe", result.get("name"));
        assertEquals(30, result.get("age"));
        assertEquals(new BigDecimal("12345.67"), result.get("salary"));
        assertEquals(Date.valueOf("1990-01-01"), result.get("birthdate"));
        assertEquals(Timestamp.valueOf("2023-07-10 10:30:00"), result.get("created_time"));
    }

    @Test
    void testNormalizeInsertData_emptyNumericField_shouldConvertToNull() throws Exception {
        Map<String, Integer> columnTypes = Map.of(
                "age", Types.INTEGER,
                "salary", Types.DECIMAL
        );

        Map<String, String> rawData = new HashMap<>();
        rawData.put("age", "");
        rawData.put("salary", null);

        MetadataRepository spyRepo = Mockito.spy(metadataRepository);
        doReturn(columnTypes).when(spyRepo).getColumnTypes("employee");

        Map<String, Object> result = spyRepo.normalizeInsertData("employee", rawData, rawData.keySet());

        assertNull(result.get("age"));
        assertNull(result.get("salary"));
    }


    @Test
    void testNormalizeInsertData_invalidNumber_shouldFallbackToRawString() throws Exception {
        Map<String, Integer> columnTypes = Map.of("age", Types.INTEGER);

        Map<String, String> rawData = Map.of("age", "thirty");

        MetadataRepository spyRepo = Mockito.spy(metadataRepository);
        doReturn(columnTypes).when(spyRepo).getColumnTypes("employee");

        Map<String, Object> result = spyRepo.normalizeInsertData("employee", rawData, rawData.keySet());

        assertEquals("thirty", result.get("age"));
    }
    
    
    @Test
    void testGetColumnTypes_returnsCorrectMap() throws Exception {
 
        String tableName = "employee";

        when(jdbcTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("SELECT * FROM `" + tableName + "` LIMIT 1")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

        when(resultSetMetaData.getColumnCount()).thenReturn(2);
        when(resultSetMetaData.getColumnName(1)).thenReturn("id");
        when(resultSetMetaData.getColumnType(1)).thenReturn(Types.INTEGER);
        when(resultSetMetaData.getColumnName(2)).thenReturn("name");
        when(resultSetMetaData.getColumnType(2)).thenReturn(Types.VARCHAR);

        
        Map<String, Integer> columnTypes = metadataRepository.getColumnTypes(tableName);
 
        assertEquals(2, columnTypes.size());
        assertEquals(Types.INTEGER, columnTypes.get("id"));
        assertEquals(Types.VARCHAR, columnTypes.get("name"));
    }
    
    @Test
    void testGetDataByPolicyId() {
        SearchRequest req = buildSearchRequest();
        String table = "employee";
        List<Map<String, Object>> mockData = List.of(Map.of("id", 1, "name", "Alice"));

        when(jdbcTemplate.queryForList(
                anyString(),
                eq(req.getPolicyId()), eq(req.getSize()), eq(req.getPage() * req.getSize())))
            .thenReturn(mockData);

        List<Map<String, Object>> result = metadataRepository.getDataByPolicyId(table, req);

        assertEquals(1, result.size());
        assertEquals("Alice", result.get(0).get("name"));
    }

    @Test
    void testGetDataByOrgId() {
        SearchRequest req = buildSearchRequest();
        String table = "employee";
        List<Map<String, Object>> mockData = List.of(Map.of("id", 2, "name", "Bob"));

        when(jdbcTemplate.queryForList(
                anyString(),
                eq(req.getOrganizationId()), eq(req.getSize()), eq(req.getPage() * req.getSize())))
            .thenReturn(mockData);

        List<Map<String, Object>> result = metadataRepository.getDataByOrgId(table, req);

        assertEquals(1, result.size());
        assertEquals("Bob", result.get(0).get("name"));
    }

    @Test
    void testGetDataByPolicyAndOrgId() {
        SearchRequest req = buildSearchRequest();
        String table = "employee";
        List<Map<String, Object>> mockData = List.of(Map.of("id", 3, "name", "Charlie"));

        when(jdbcTemplate.queryForList(
                anyString(),
                eq(req.getPolicyId()), eq(req.getOrganizationId()), eq(req.getSize()), eq(req.getPage() * req.getSize())))
            .thenReturn(mockData);

        List<Map<String, Object>> result = metadataRepository.getDataByPolicyAndOrgId(table, req);

        assertEquals(1, result.size());
        assertEquals("Charlie", result.get(0).get("name"));
    }

   
}
