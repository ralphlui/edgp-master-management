package sg.edu.nus.iss.edgp.masterdata.management.dto;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class UploadResult {
	private String message;
    private int totalRecord;
    private List<Map<String, Object>> data;
 

}
