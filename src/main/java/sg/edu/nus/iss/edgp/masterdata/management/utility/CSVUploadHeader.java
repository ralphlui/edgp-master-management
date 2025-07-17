package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class CSVUploadHeader {

	private final String id;
    private final String fileName;
    private final String uploadedBy;
    private final String uploadDate;

    public CSVUploadHeader(String id, String fileName, String uploadedBy) {
        this.id = id;
        this.fileName = fileName;
        this.uploadedBy = uploadedBy;
        this.uploadDate = LocalDateTime.now().toString();
    }

    public Map<String, AttributeValue> toItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(id).build());
        item.put("fileName", AttributeValue.builder().s(fileName).build());
        item.put("uploadedBy", AttributeValue.builder().s(uploadedBy).build());
        item.put("uploadDate", AttributeValue.builder().s(uploadDate).build());
        return item;
    }
}
