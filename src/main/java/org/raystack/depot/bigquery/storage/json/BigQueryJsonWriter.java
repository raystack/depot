package org.raystack.depot.bigquery.storage.json;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import org.raystack.depot.bigquery.storage.BigQueryPayload;
import org.raystack.depot.bigquery.storage.BigQueryWriter;
import org.raystack.depot.config.BigQuerySinkConfig;

import java.util.concurrent.ExecutionException;

public class BigQueryJsonWriter implements BigQueryWriter {

    public BigQueryJsonWriter(BigQuerySinkConfig config) {

    }

    @Override
    public void init() {

    }

    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
