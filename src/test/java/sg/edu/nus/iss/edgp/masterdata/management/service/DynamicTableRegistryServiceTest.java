package sg.edu.nus.iss.edgp.masterdata.management.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.entity.DynamicTableRegistry;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.repository.DynamicTableRegistryRepository;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicTableRegistryService;

@ExtendWith(MockitoExtension.class)
class DynamicTableRegistryServiceTest {

	@InjectMocks
    private DynamicTableRegistryService service;

    @Mock
    private DynamicTableRegistryRepository categoryRepository;

    @Test
    void testFindCategories_returnsList() {
        List<DynamicTableRegistry> mockEntities = List.of(
            new DynamicTableRegistry("vendor", "322"),
            new DynamicTableRegistry("product", "121")
        );

        when(categoryRepository.findAll()).thenReturn(mockEntities);

        List<String> result = service.findCategories();

        assertEquals(2, result.size());
    }

    @Test
    void testFindCategories_emptyList_returnsEmpty() {
        when(categoryRepository.findAll()).thenReturn(Collections.emptyList());

        List<String> result = service.findCategories();

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }


    @Test
    void testFindCategories_throwsException() {
        when(categoryRepository.findAll()).thenThrow(new RuntimeException("DB Error"));

        MasterdataServiceException exception = assertThrows(MasterdataServiceException.class, () -> {
            service.findCategories();
        });

        assertEquals("An error occured while findCategories", exception.getMessage());
    }
}
