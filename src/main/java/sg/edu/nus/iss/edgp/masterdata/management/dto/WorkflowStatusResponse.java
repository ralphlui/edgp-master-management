package sg.edu.nus.iss.edgp.masterdata.management.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowStatusResponse {
    private String fileId;
    private int totalRecords;
    private int processedRecords;
    private String status; // "DONE" or "PROCESSING"
}
