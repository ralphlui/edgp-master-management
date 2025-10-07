package sg.edu.nus.iss.edgp.masterdata.management.utility;


import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.MasterDataHeader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class CSVUploadHeaderTest {

    @Test
    void toItem_mapsAllFields_correctly_and_formatsDates() {
      
        MasterDataHeader header = new MasterDataHeader();
        header.setId("H-123");
        header.setFileName("data.csv");
        header.setDomainName("customer");
        header.setOrganizationId("ORG-9");
        header.setPolicyId("POL-7");
        header.setUploadedBy("user@example.com");
        header.setTotalRowsCount(42);
        header.setProcessStage(FileProcessStage.PROCESSING);
        header.setFileStatus("IN_PROGRESS");

        LocalDateTime before = LocalDateTime.now(ZoneId.of("Asia/Singapore"));

        CSVUploadHeader csvHeader = new CSVUploadHeader(header);

        Map<String, AttributeValue> item = csvHeader.toItem();

        
        assertEquals("H-123", item.get("id").s());
        assertEquals("data.csv", item.get("file_name").s());
        assertEquals("customer", item.get("domain_name").s());
        assertEquals("ORG-9", item.get("organization_id").s());
        assertEquals("POL-7", item.get("policy_id").s());
        assertEquals("user@example.com", item.get("uploaded_by").s());
        assertEquals("42", item.get("total_rows_count").n());
        assertEquals(FileProcessStage.PROCESSING.toString(), item.get("process_stage").s());
        assertEquals("IN_PROGRESS", item.get("file_status").s());

        assertEquals("", item.get("updated_date").s());

        
        String uploaded = item.get("uploaded_date").s();
        assertNotNull(uploaded);
        assertTrue(Pattern.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", uploaded),
                "uploaded_date should match yyyy-MM-dd HH:mm:ss but was: " + uploaded);

        LocalDateTime parsed = LocalDateTime.parse(uploaded, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime after = LocalDateTime.now(ZoneId.of("Asia/Singapore"));
        assertFalse(parsed.isBefore(before.minusSeconds(5)), "uploaded_date should not be earlier than a few seconds before test start");
        assertFalse(parsed.isAfter(after.plusMinutes(2)), "uploaded_date should be reasonably close to 'now'");

        assertEquals(11, item.size(), "Expected exactly 11 attributes in the item");
    }
}

