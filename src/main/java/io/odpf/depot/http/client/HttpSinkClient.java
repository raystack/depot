package io.odpf.depot.http.client;

import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.response.HttpSinkResponse;
import io.odpf.depot.metrics.Instrumentation;
import org.apache.http.client.HttpClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HttpSinkClient implements Closeable {

    private final HttpClient httpClient;
    private final Instrumentation instrumentation;

    public HttpSinkClient(HttpClient httpClient, Instrumentation instrumentation) {
        this.httpClient = httpClient;
        this.instrumentation = instrumentation;
    }

    public List<HttpSinkResponse> send(List<HttpRequestRecord> records) throws IOException {
        List<HttpSinkResponse> responseList = new ArrayList<>();
        for (HttpRequestRecord record : records) {
            HttpSinkResponse sinkResponse = record.send(httpClient);
            responseList.add(sinkResponse);
        }
        return responseList;
    }

    @Override
    public void close() throws IOException {
    }
}
