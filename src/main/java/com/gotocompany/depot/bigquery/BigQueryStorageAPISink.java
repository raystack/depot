package com.gotocompany.depot.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageResponseParser;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BigQueryStorageAPISink implements Sink {
    private final BigQueryStorageClient bigQueryStorageClient;
    private final BigQueryStorageResponseParser responseParser;

    public BigQueryStorageAPISink(
            BigQueryStorageClient bigQueryStorageClient,
            BigQueryStorageResponseParser responseParser) {
        this.bigQueryStorageClient = bigQueryStorageClient;
        this.responseParser = responseParser;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        SinkResponse sinkResponse = new SinkResponse();
        BigQueryPayload payload = bigQueryStorageClient.convert(messages);
        responseParser.setSinkResponseForInvalidMessages(payload, messages, sinkResponse);
        if (payload.getPayloadIndexes().size() > 0) {
            try {
                AppendRowsResponse appendRowsResponse = bigQueryStorageClient.appendAndGet(payload);
                responseParser.setSinkResponseForErrors(payload, appendRowsResponse, messages, sinkResponse);
            } catch (ExecutionException e) {
                e.printStackTrace();
                Throwable cause = e.getCause();
                responseParser.setSinkResponseForException(cause, payload, messages, sinkResponse);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new SinkException("Interrupted exception occurred", e);
            }
        }
        return sinkResponse;
    }

    @Override
    public void close() throws IOException {
        bigQueryStorageClient.close();
    }
}
