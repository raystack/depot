package io.odpf.sink.connectors.expcetion;

public class ProtoNotFoundException extends RuntimeException {
    public ProtoNotFoundException(String message) {
        super(message);
    }
}
