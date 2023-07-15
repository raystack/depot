package org.raystack.depot.exception;

import java.io.IOException;

public class RaystackSinkException extends IOException {
    public RaystackSinkException(String message, Throwable th) {
        super(message, th);
    }
}
