package sg.edu.nus.iss.edgp.masterdata.management.exception;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError; 
import org.springframework.core.MethodParameter; 
import org.springframework.validation.BindingResult; 
import org.springframework.web.bind.MethodArgumentNotValidException;

import sg.edu.nus.iss.edgp.masterdata.management.dto.APIResponse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Arrays;
 

@ExtendWith(MockitoExtension.class)
class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler exceptionHandler;

    @BeforeEach
    void setup() {
        exceptionHandler = new GlobalExceptionHandler();
    }


    @Test
    void testHandleGenericException() {
        Exception ex = new Exception("Some error");
        ResponseEntity<APIResponse> response = exceptionHandler.handleObjectNotFoundException(ex);

        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
        assertEquals("Failed to get data. Some error", response.getBody().getMessage());
    }

    @Test
    void testIllegalArgumentException() {
        IllegalArgumentException ex = new IllegalArgumentException("Invalid ID");
        ResponseEntity<APIResponse> response = exceptionHandler.illegalArgumentException(ex);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("Invalid data: Invalid ID", response.getBody().getMessage());
    }
    
    
    @Test
    void testHandleValidationException_forNormalField() throws Exception {
        // Mock FieldError
        FieldError fieldError = new FieldError("dummyObject", "category", "must not be blank");

        // Mock BindingResult
        BindingResult bindingResult = mock(BindingResult.class);
        when(bindingResult.getFieldErrors()).thenReturn(Arrays.asList(fieldError));

        // Create dummy MethodArgumentNotValidException
        Method dummyMethod = DummyController.class.getMethod("dummyMethod", DummyDTO.class);
        MethodArgumentNotValidException ex = new MethodArgumentNotValidException(
                new MethodParameter(dummyMethod, 0), bindingResult);

        // Act
        ResponseEntity<APIResponse> response = exceptionHandler.handleValidationException(ex);

        // Assert
        assertEquals(400, response.getStatusCodeValue());
        assertTrue(response.getBody().getMessage().contains("Invalid value for 'category'"));
    }

    @Test
    void testHandleValidationException_forPageField() throws Exception {
        FieldError fieldError = new FieldError("dummyObject", "page", "must be positive");

        BindingResult bindingResult = mock(BindingResult.class);
        when(bindingResult.getFieldErrors()).thenReturn(Arrays.asList(fieldError));

        Method dummyMethod = DummyController.class.getMethod("dummyMethod", DummyDTO.class);
        MethodArgumentNotValidException ex = new MethodArgumentNotValidException(
                new MethodParameter(dummyMethod, 0), bindingResult);

        ResponseEntity<APIResponse> response = exceptionHandler.handleValidationException(ex);

        assertEquals(400, response.getStatusCodeValue());
        assertTrue(response.getBody().getMessage().contains("The 'page' field must be a valid positive integer"));
    }
    
    static class DummyController {
        public void dummyMethod(DummyDTO dto) {}
    }

    static class DummyDTO {
        private String category;
    }

}
