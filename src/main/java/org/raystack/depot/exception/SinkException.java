package org.raystack.depot.exception;

import java.io.IOException;

public class SinkException extends IOException {
    public SinkException(String message, Throwable th) {
        super(message, th);
    }
}
