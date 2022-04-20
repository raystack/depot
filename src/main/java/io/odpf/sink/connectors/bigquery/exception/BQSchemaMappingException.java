package io.odpf.sink.connectors.bigquery.exception;

public class BQSchemaMappingException extends RuntimeException {
    public BQSchemaMappingException(String message) {
        super(message);
    }
}
