package com.ritikbansod.kafkawrapper.web;

import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Uniform JSON error responses: {error, detail}. Kafka-specific exceptions map
 * to meaningful HTTP statuses instead of blanket 500s.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler({NoSuchElementException.class, UnknownTopicOrPartitionException.class,
            GroupIdNotFoundException.class, NoResourceFoundException.class})
    public ResponseEntity<Map<String, Object>> notFound(Exception e) {
        return body(HttpStatus.NOT_FOUND, e.getClass().getSimpleName(), message(e));
    }

    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentNotValidException.class,
            TopicExistsException.class})
    public ResponseEntity<Map<String, Object>> badRequest(Exception e) {
        String detail = e instanceof MethodArgumentNotValidException manv
                ? manv.getBindingResult().getAllErrors().stream()
                        .map(err -> err.getDefaultMessage()).reduce((a, b) -> a + "; " + b).orElse("invalid request")
                : message(e);
        return body(HttpStatus.BAD_REQUEST, e.getClass().getSimpleName(), detail);
    }

    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<Map<String, Object>> timeout(TimeoutException e) {
        return body(HttpStatus.GATEWAY_TIMEOUT, "Timeout", message(e));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> conflict(IllegalStateException e) {
        return body(HttpStatus.CONFLICT, "IllegalState", message(e));
    }

    /**
     * The AdminClient wraps nearly every Kafka error in ExecutionException —
     * unwrap it so the underlying cause gets its proper status code.
     */
    @ExceptionHandler(ExecutionException.class)
    public ResponseEntity<Map<String, Object>> execution(ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof NoSuchElementException || cause instanceof UnknownTopicOrPartitionException
                || cause instanceof GroupIdNotFoundException) {
            return body(HttpStatus.NOT_FOUND, cause.getClass().getSimpleName(), message(cause));
        }
        if (cause instanceof IllegalArgumentException || cause instanceof TopicExistsException) {
            return body(HttpStatus.BAD_REQUEST, cause.getClass().getSimpleName(), message(cause));
        }
        if (cause instanceof java.util.concurrent.TimeoutException) {
            return body(HttpStatus.GATEWAY_TIMEOUT, "Timeout", message(cause));
        }
        log.error("Kafka operation failed", cause);
        return body(HttpStatus.INTERNAL_SERVER_ERROR, cause.getClass().getSimpleName(), message(cause));
    }

    /** Malformed JSON body, wrong field types, missing body. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> unreadable(HttpMessageNotReadableException e) {
        return body(HttpStatus.BAD_REQUEST, "MalformedRequestBody", "Request body is missing or not valid JSON: "
                + message(e));
    }

    /** Wrong types in path variables / request params (e.g. /brokers/abc/configs). */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Map<String, Object>> typeMismatch(MethodArgumentTypeMismatchException e) {
        return body(HttpStatus.BAD_REQUEST, "TypeMismatch",
                "Parameter '" + e.getName() + "' has an invalid value");
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> missingParam(MissingServletRequestParameterException e) {
        return body(HttpStatus.BAD_REQUEST, "MissingParameter", "Required parameter '" + e.getParameterName() + "' is missing");
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<Map<String, Object>> methodNotSupported(HttpRequestMethodNotSupportedException e) {
        return body(HttpStatus.METHOD_NOT_ALLOWED, "MethodNotAllowed", message(e));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> general(Exception e) {
        log.error("Unhandled error on {} {}", "", e.getMessage(), e);
        return body(HttpStatus.INTERNAL_SERVER_ERROR, e.getClass().getSimpleName(), message(e));
    }

    private ResponseEntity<Map<String, Object>> body(HttpStatus status, String error, String detail) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", status.value());
        payload.put("error", error);
        payload.put("detail", detail);
        return ResponseEntity.status(status).body(payload);
    }

    private static String message(Throwable e) {
        while (e.getCause() != null && (e.getMessage() == null || e.getMessage().isBlank())) {
            e = e.getCause();
        }
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }
}
