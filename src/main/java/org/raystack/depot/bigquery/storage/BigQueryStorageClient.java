package org.raystack.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import org.raystack.depot.message.Message;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface BigQueryStorageClient extends Closeable {
    BigQueryPayload convert(List<Message> messages);

    AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException;
}
