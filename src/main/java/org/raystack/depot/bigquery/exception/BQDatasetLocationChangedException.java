package org.raystack.depot.bigquery.exception;

public class BQDatasetLocationChangedException extends RuntimeException {
    public BQDatasetLocationChangedException(String message) {
        super(message);
    }
}
