package com.pinnacle.payloadconsumer.exception;

public class PayloadConsumerException extends RuntimeException {
    public PayloadConsumerException(String message) {
        super(message);
    }

    public PayloadConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
