package io.odpf.depot.expcetion;

import java.io.IOException;

public class OdpfSinkException extends IOException {
    public OdpfSinkException(String message, Throwable th) {
        super(message, th);
    }
}
