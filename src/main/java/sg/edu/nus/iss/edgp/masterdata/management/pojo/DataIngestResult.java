package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class DataIngestResult {
	private String message;
    private int totalRecord;
}
