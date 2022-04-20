package io.odpf.sink.connectors.bigquery.exception;

public class BQPartitionKeyNotSpecified extends RuntimeException {
    public BQPartitionKeyNotSpecified(String message) {
        super(message);
    }
}
