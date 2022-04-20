package io.odpf.sink.connectors.bigquery.exception;

public class BQDatasetLocationChangedException extends RuntimeException {
    public BQDatasetLocationChangedException(String message) {
        super(message);
    }
}

