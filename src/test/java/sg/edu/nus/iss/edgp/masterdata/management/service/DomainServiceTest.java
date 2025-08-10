package sg.edu.nus.iss.edgp.masterdata.management.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import static org.mockito.Mockito.any;

import static org.mockito.Mockito.anyString;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DomainService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@ExtendWith(MockitoExtension.class)
class DomainServiceTest {

    @InjectMocks
    private DomainService domainService;

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Mock
    private DynamicDetailService dynamicDetailService;

    @Test
    void testFindCategories_success() {
 
        when(dynamicDetailService.tableExists(anyString())).thenReturn(true);
        Map<String, AttributeValue> item1 = Map.of("name", AttributeValue.builder().s("Vendor").build());
        Map<String, AttributeValue> item2 = Map.of("name", AttributeValue.builder().s("Product").build());

        ScanResponse mockResponse = ScanResponse.builder()
                .items(List.of(item1, item2))
                .build();

        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(mockResponse);

        List<String> categories = domainService.findDomains();

        assertEquals(2, categories.size());
        assertTrue(categories.contains("Vendor"));
        assertTrue(categories.contains("Product"));
    }

    @Test
    void testFindCategories_tableNotExists_returnsEmptyList() {
        when(dynamicDetailService.tableExists(anyString())).thenReturn(false);

        List<String> result = domainService.findDomains();

        assertTrue(result.isEmpty());
    }

    @Test
    void testFindCategories_exceptionThrown() {
        when(dynamicDetailService.tableExists(anyString())).thenReturn(true);
        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenThrow(RuntimeException.class);

        assertThrows(MasterdataServiceException.class, () -> {
        	domainService.findDomains();
        });
    }
}
