package sg.edu.nus.iss.edgp.masterdata.management.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import sg.edu.nus.iss.edgp.masterdata.management.dto.APIResponse;



public class GlobalExceptionHandler {
	
	 private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

	 @SuppressWarnings("rawtypes")
		@ExceptionHandler(Exception.class)
		@ResponseBody
		ResponseEntity<APIResponse> handleObjectNotFoundException(Exception ex){
			String message = "Failed to get data. " + ex.getMessage();
			 logger.error(message);
			  return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
						.body(APIResponse.error(message));
		}
		
		
		@SuppressWarnings("rawtypes")
		@ExceptionHandler(IllegalArgumentException.class)
	    @ResponseBody
	    public ResponseEntity<APIResponse> illegalArgumentException(IllegalArgumentException ex) {
	        String message = "Invalid data: " + ex.getMessage();
	        logger.error(message);
	        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
	                .body(APIResponse.error(message));
	    }
		
		
		@SuppressWarnings("rawtypes")
		@ExceptionHandler(MethodArgumentNotValidException.class)
		@ResponseBody
		public ResponseEntity<APIResponse> handleValidationException(MethodArgumentNotValidException ex) {
			StringBuilder errorMessage = new StringBuilder("Validation failed: ");

			for (FieldError error : ex.getBindingResult().getFieldErrors()) {
				String fieldName = error.getField();
				String defaultMessage = error.getDefaultMessage();

				if ("page".equals(fieldName) || "size".equals(fieldName)) {
					errorMessage.append(String.format("The '%s' field must be a valid positive integer. ", fieldName));
				} else {
					errorMessage.append(String.format("Invalid value for '%s': %s. ", fieldName, defaultMessage));
				}
			}

			String message = errorMessage.toString();
			logger.error(message);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(APIResponse.error(message));																							
		}
}
