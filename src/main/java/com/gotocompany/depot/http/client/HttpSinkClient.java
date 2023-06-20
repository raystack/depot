package com.gotocompany.depot.http.client;

import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.HttpSinkMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.client.HttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HttpSinkClient {

    private final HttpClient httpClient;
    private final Instrumentation instrumentation;
    private final HttpSinkMetrics httpSinkMetrics;

    public HttpSinkClient(HttpClient httpClient, HttpSinkMetrics httpSinkMetrics, Instrumentation instrumentation) {
        this.httpClient = httpClient;
        this.instrumentation = instrumentation;
        this.httpSinkMetrics = httpSinkMetrics;
    }

    public List<HttpSinkResponse> send(List<HttpRequestRecord> records) throws IOException {
        List<HttpSinkResponse> responseList = new ArrayList<>();
        for (HttpRequestRecord record : records) {
            HttpSinkResponse sinkResponse = record.send(httpClient);
            responseList.add(sinkResponse);
            instrument(sinkResponse);
        }
        return responseList;
    }

    private void instrument(HttpSinkResponse httpSinkResponse) {
        String statusCode = httpSinkResponse.getResponseCode();
        String httpCodeTag = statusCode.equals("null") ? "status_code=" : "status_code=" + statusCode;
        instrumentation.captureCount(httpSinkMetrics.getHttpResponseCodeTotalMetric(), 1L, httpCodeTag);

    }

}
