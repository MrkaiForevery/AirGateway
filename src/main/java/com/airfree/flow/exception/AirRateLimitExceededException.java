package com.airfree.flow.exception;

public class AirRateLimitExceededException extends RuntimeException{

    public AirRateLimitExceededException(String message) {
        super(message);
    }
}
