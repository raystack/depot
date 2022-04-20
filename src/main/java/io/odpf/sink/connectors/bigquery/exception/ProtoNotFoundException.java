package io.odpf.sink.connectors.bigquery.exception;

public class ProtoNotFoundException extends RuntimeException {
    public ProtoNotFoundException(String message) {
        super(message);
    }
}
