package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@Setter
@Getter
public class TemplateFileFormat {
	private String fieldName ="";
	private String description ="" ;
	private String dataType="";
	private int length = 0;
 
}
