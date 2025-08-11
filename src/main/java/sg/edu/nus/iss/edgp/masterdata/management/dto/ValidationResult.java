package sg.edu.nus.iss.edgp.masterdata.management.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValidationResult {
	private boolean isValid;
	private String message;
}