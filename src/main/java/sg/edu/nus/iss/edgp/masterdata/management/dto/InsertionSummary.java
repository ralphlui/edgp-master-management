package sg.edu.nus.iss.edgp.masterdata.management.dto;

import java.util.List;
import java.util.Map;

public record InsertionSummary(int totalInserted, List<Map<String, Object>> previewTop50) {
}
