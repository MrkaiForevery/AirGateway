package com.airfree.flow.exception;


public class AirCircuitBreakerOpenException extends RuntimeException {

    public AirCircuitBreakerOpenException(String message) {
        super(message);
    }
}
