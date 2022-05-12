package io.odpf.depot.exception;

import java.io.IOException;

public class OdpfSinkException extends IOException {
    public OdpfSinkException(String message, Throwable th) {
        super(message, th);
    }
}
