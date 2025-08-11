package sg.edu.nus.iss.edgp.masterdata.management;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class EdgpMasterdataManagementApplication {

	public static void main(String[] args) {
		SpringApplication.run(EdgpMasterdataManagementApplication.class, args);
	}

}
