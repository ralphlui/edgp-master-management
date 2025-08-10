package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

@Component
public class CSVParser {
	private static String normalizeHeader(String header) {
	    if (header == null) return "";

	    // Remove BOM, trim spaces
	    String cleaned = header.replace("\uFEFF", "").trim();

	    // Replace spaces/hyphens with underscores
	    cleaned = cleaned.replaceAll("[\\s-]+", "_");

	    // Convert camelCase or PascalCase to snake_case
	    cleaned = cleaned.replaceAll("([a-z])([A-Z])", "$1_$2");

	    // Lowercase everything
	    cleaned = cleaned.toLowerCase();

	    return cleaned;
	}

	public List<Map<String, String>> parseCsv(MultipartFile file) throws IOException {
	    List<Map<String, String>> result = new ArrayList<>();
	    if (file == null || file.isEmpty()) return result;

	    try (BufferedReader reader = new BufferedReader(
	            new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {

	        String headerLine = reader.readLine();
	        if (headerLine == null) return result;

	        String[] headers = headerLine.split(",", -1);
	        for (int i = 0; i < headers.length; i++) {
	            headers[i] = normalizeHeader(headers[i]);
	        }

	        String line;
	        String csvSafeSplit = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	        while ((line = reader.readLine()) != null) {
	            String[] values = line.split(csvSafeSplit, -1);
	            Map<String, String> row = new LinkedHashMap<>();
	            for (int i = 0; i < headers.length; i++) {
	                String key = headers[i];
	                String val = i < values.length ? values[i].replaceAll("^\"|\"$", "").trim() : "";
	                row.put(key, val);
	            }
	            result.add(row);
	        }
	    }
	    return result;
	}

}
