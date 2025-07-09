package sg.edu.nus.iss.edgp.masterdata.management.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
 

@ExtendWith(MockitoExtension.class)
public class MasterdataServiceExceptionTest {
	@Test
	void testConstructor() {

		String errorMessage = "Category not found";
		MasterdataServiceException exception = new MasterdataServiceException(errorMessage);

		assertEquals(errorMessage, exception.getMessage());
	}

	@Test
	void testConstructorWithMessageAndCause() {
		String message = "Category not found";
		Throwable cause = new RuntimeException("Database error");
		MasterdataServiceException exception = new MasterdataServiceException(message, cause);

		assertEquals(message, exception.getMessage());
		assertEquals(cause, exception.getCause());
	}
}
