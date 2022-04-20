package io.odpf.sink.connectors.bigquery.exception;

public class BQTableUpdateFailure extends RuntimeException {
    public BQTableUpdateFailure(String message, Throwable rootCause) {
        super(message, rootCause);
    }
}
