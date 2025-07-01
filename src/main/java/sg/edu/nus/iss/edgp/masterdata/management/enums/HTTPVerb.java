package sg.edu.nus.iss.edgp.masterdata.management.enums;

public enum HTTPVerb {
	GET, POST, PUT, PATCH;
	
	  public static HTTPVerb fromString(String method) {
        for (HTTPVerb verb : HTTPVerb.values()) {
            if (verb.name().equalsIgnoreCase(method)) {
                return verb;
            }
        }
        throw new IllegalArgumentException("No enum constant for method: " + method);
    }
}

