package sg.edu.nus.iss.edgp.masterdata.management.exception;

public class MasterdataServiceException extends RuntimeException {
	
	public MasterdataServiceException(String message) {
		 super(message);
	}
	
	public MasterdataServiceException (String message, Throwable cause) {
        super(message, cause);
    }

}
