package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;

import java.util.concurrent.ExecutionException;

public interface BigQueryWriter extends AutoCloseable {

    void init();

    AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException;
}
