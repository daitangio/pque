package com.gioorgi.pque.client;

public class PGMQException extends RuntimeException {

    public PGMQException(String message) {
        super(message);
    }

    public PGMQException(String message, Throwable cause) {
        super(message, cause);
    }

}
