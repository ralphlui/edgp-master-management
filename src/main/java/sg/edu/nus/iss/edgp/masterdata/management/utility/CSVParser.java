package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

@Component
public class CSVParser {
	public List<Map<String, String>> parseCsv(MultipartFile file) throws IOException {
	    List<Map<String, String>> result = new ArrayList<>();

	    try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
	        String[] headers = reader.readLine().split(",");
	        String line;

	        while ((line = reader.readLine()) != null) {
	            String[] values = line.split(",", -1);
	            Map<String, String> row = new HashMap<>();
	            for (int i = 0; i < headers.length; i++) {
	                row.put(headers[i].trim(), i < values.length ? values[i].trim() : "");
	            }
	            result.add(row);
	        }
	    }

	    return result;
	}
}
