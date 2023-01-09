package io.odpf.depot.http.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.http.response.HttpSinkResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class HttpRequestRecord implements Iterable<Integer> {

    private final Set<Integer> recordIndexes = new HashSet<>();
    private final ErrorInfo errorInfo;
    private final boolean valid;
    private final HttpEntityEnclosingRequestBase httpRequest;

    public HttpRequestRecord(ErrorInfo errorInfo, boolean valid, HttpEntityEnclosingRequestBase httpRequest) {
        this.errorInfo = errorInfo;
        this.valid = valid;
        this.httpRequest = httpRequest;
    }

    public HttpRequestRecord(HttpEntityEnclosingRequestBase httpRequest) {
        this(null, true, httpRequest);
    }

    public HttpRequestRecord(ErrorInfo errorInfo) {
        this(errorInfo, false, null);
    }


    public HttpSinkResponse send(HttpClient httpClient) throws IOException {
        HttpResponse response = httpClient.execute(httpRequest);
        return new HttpSinkResponse(response);
    }

    public String getRequestBody() throws IOException {
        return EntityUtils.toString(httpRequest.getEntity());
    }

    public void addIndex(Integer index) {
        recordIndexes.add(index);
    }

    public void addAllIndexes(Set<Integer> indexes) {
        recordIndexes.addAll(indexes);
    }

    public ErrorInfo getErrorInfo() {
        return errorInfo;
    }

    public boolean isValid() {
        return valid;
    }

    public String printRequest() throws IOException {
        return String.format("\nRequest Method: %s\nRequest Url: %s\nRequest Headers: %s\nRequest Body: %s",
                httpRequest.getMethod(),
                httpRequest.getURI(),
                Arrays.asList(httpRequest.getAllHeaders()),
                getRequestBody());
    }

    @NotNull
    @Override
    public Iterator<Integer> iterator() {
        return recordIndexes.iterator();
    }
}
