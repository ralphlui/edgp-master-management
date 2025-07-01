package sg.edu.nus.iss.edgp.masterdata.management.pojo;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {

	private String userId = "";

	private String email = "";
	private String username = "";

	private String password = "";

	private String role = "";

}