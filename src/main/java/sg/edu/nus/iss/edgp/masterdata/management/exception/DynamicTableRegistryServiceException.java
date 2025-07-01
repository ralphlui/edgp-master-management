package sg.edu.nus.iss.edgp.masterdata.management.exception;

public class DynamicTableRegistryServiceException extends RuntimeException {
	
	public DynamicTableRegistryServiceException(String message) {
		 super(message);
	}
	
	public DynamicTableRegistryServiceException (String message, Throwable cause) {
        super(message, cause);
    }

}
