package org.openphc.cce.intelligence.web;

import jakarta.servlet.http.HttpServletRequest;
import org.openphc.cce.intelligence.web.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.OffsetDateTime;
import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, HttpServletRequest request) {
        String message = ex.getMessage();
        // "not found" messages should return 404
        if (message != null && message.toLowerCase().contains("not found")) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ErrorResponse.of("NOT_FOUND", message, request.getRequestURI()));
        }
        return ResponseEntity.badRequest()
                .body(ErrorResponse.of("BAD_REQUEST", message, request.getRequestURI()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> handleIllegalState(IllegalStateException ex, HttpServletRequest request) {
        String message = ex.getMessage();
        // Conflict for duplicates and resource constraint violations
        if (message != null && (message.toLowerCase().contains("already exists")
                || message.toLowerCase().contains("duplicate")
                || message.toLowerCase().contains("cannot delete"))) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(ErrorResponse.of("CONFLICT", message, request.getRequestURI()));
        }
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(ErrorResponse.of("UNPROCESSABLE_ENTITY", message, request.getRequestURI()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        List<ErrorResponse.FieldError> fieldErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> ErrorResponse.FieldError.builder()
                        .field(fe.getField())
                        .message(fe.getDefaultMessage())
                        .build())
                .toList();

        ErrorResponse response = ErrorResponse.builder()
                .error(ErrorResponse.ErrorBody.builder()
                        .code("BAD_REQUEST")
                        .message("Validation failed")
                        .path(request.getRequestURI())
                        .timestamp(OffsetDateTime.now())
                        .fieldErrors(fieldErrors)
                        .build())
                .build();
        return ResponseEntity.badRequest().body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneral(Exception ex, HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of("INTERNAL_ERROR", "An unexpected error occurred", request.getRequestURI()));
    }
}
