package com.gotocompany.depot.bigquery.exception;

public class BQPartitionKeyNotSpecified extends RuntimeException {
    public BQPartitionKeyNotSpecified(String message) {
        super(message);
    }
}
