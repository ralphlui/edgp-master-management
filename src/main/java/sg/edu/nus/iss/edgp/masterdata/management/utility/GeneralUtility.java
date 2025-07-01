package sg.edu.nus.iss.edgp.masterdata.management.utility;

import org.springframework.stereotype.Component;

@Component
public class GeneralUtility {

	public static String makeNotNull(Object str) {
		if (str == null) {
			return "";
		} else if (str.equals("null")) {
			return "";
		} else {
			return str.toString();
		}
	}

}