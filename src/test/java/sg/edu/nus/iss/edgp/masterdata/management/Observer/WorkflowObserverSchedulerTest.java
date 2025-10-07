package sg.edu.nus.iss.edgp.masterdata.management.Observer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.DynamicDetailService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.HeaderService;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;

@ExtendWith(MockitoExtension.class)
class WorkflowObserverSchedulerTest {

    private HeaderService headerService;
    private MasterdataService masterdataService;
    private DynamicDetailService dynamoService;

    private WorkflowObserverScheduler scheduler;

    @BeforeEach
    void setUp() {
        headerService = mock(HeaderService.class);
        masterdataService = mock(MasterdataService.class);
        dynamoService = mock(DynamicDetailService.class);

        scheduler = new WorkflowObserverScheduler(headerService, masterdataService, dynamoService);

        ReflectionTestUtils.setField(scheduler, "headerTableName", "md_header");
        ReflectionTestUtils.setField(scheduler, "stagingTableName", "md_staging");
    }

    @Test
    void whenNoProcessingFile_dispatchesNextBatch() {
        when(dynamoService.tableExists("md_header")).thenReturn(true);
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
        when(headerService.fetchOldestByStage(FileProcessStage.PROCESSING))
                .thenReturn(Optional.empty());
        when(masterdataService.processAndSendRawDataToSqs()).thenReturn(3);

        assertDoesNotThrow(() -> scheduler.checkWorkflowStatusAndPushNext());

        verify(headerService).fetchOldestByStage(FileProcessStage.PROCESSING);
        verify(masterdataService).processAndSendRawDataToSqs();
    }

    @Test
    void whenProcessingFilePresent_doesNotDispatch() {
        when(dynamoService.tableExists("md_header")).thenReturn(true);
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
        MasterDataHeader hdr = new MasterDataHeader();
        when(headerService.fetchOldestByStage(FileProcessStage.PROCESSING))
                .thenReturn(Optional.of(hdr));

        assertDoesNotThrow(() -> scheduler.checkWorkflowStatusAndPushNext());

        verify(headerService).fetchOldestByStage(FileProcessStage.PROCESSING);
        verify(masterdataService, never()).processAndSendRawDataToSqs();
    }

    @Test
    void whenTablesMissing_noOp() {
       
        when(dynamoService.tableExists("md_header")).thenReturn(false);
        
        assertDoesNotThrow(() -> scheduler.checkWorkflowStatusAndPushNext());

        verify(dynamoService).tableExists("md_header");
       
        verifyNoInteractions(headerService, masterdataService);
    }

    @Test
    void whenMasterdataServiceThrows_isCaughtAndSwallowed() {
        when(dynamoService.tableExists("md_header")).thenReturn(true);
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
        when(headerService.fetchOldestByStage(FileProcessStage.PROCESSING))
                .thenReturn(Optional.empty());
        when(masterdataService.processAndSendRawDataToSqs())
                .thenThrow(new MasterdataServiceException("boom"));

        assertDoesNotThrow(() -> scheduler.checkWorkflowStatusAndPushNext());

        verify(masterdataService).processAndSendRawDataToSqs();
    }

    @Test
    void whenUnexpectedExceptionOccurs_isCaughtAndSwallowed() {
        when(dynamoService.tableExists("md_header")).thenReturn(true);
        when(dynamoService.tableExists("md_staging")).thenReturn(true);
       
        when(headerService.fetchOldestByStage(FileProcessStage.PROCESSING))
                .thenThrow(new RuntimeException("unexpected"));

        assertDoesNotThrow(() -> scheduler.checkWorkflowStatusAndPushNext());

        verify(headerService).fetchOldestByStage(FileProcessStage.PROCESSING);
        verify(masterdataService, never()).processAndSendRawDataToSqs();
    }
}
