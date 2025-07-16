package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class CsvUploadHeader {

	private final String id;
    private final String filename;
    private final String uploadedBy;
    private final String uploadDate;

    public CsvUploadHeader(String id, String filename, String uploadedBy) {
        this.id = id;
        this.filename = filename;
        this.uploadedBy = uploadedBy;
        this.uploadDate = LocalDateTime.now().toString();
    }

    public Map<String, AttributeValue> toItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(id).build());
        item.put("filename", AttributeValue.builder().s(filename).build());
        item.put("uploadedBy", AttributeValue.builder().s(uploadedBy).build());
        item.put("uploadDate", AttributeValue.builder().s(uploadDate).build());
        return item;
    }
}
