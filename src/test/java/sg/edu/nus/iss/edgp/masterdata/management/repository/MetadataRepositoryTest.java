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
