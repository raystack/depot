package io.odpf.depot.http.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.http.response.HttpSinkResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class HttpRequestRecord implements Iterable<Integer> {

    private final Set<Integer> indexes = new HashSet<>();
    private final ErrorInfo errorInfo;
    private final boolean valid;
    private final HttpEntityEnclosingRequestBase httpRequest;

    public HttpRequestRecord(ErrorInfo errorInfo, boolean valid, HttpEntityEnclosingRequestBase httpRequest) {
        this.errorInfo = errorInfo;
        this.valid = valid;
        this.httpRequest = httpRequest;
    }

    public HttpSinkResponse send(HttpClient httpClient) throws IOException {
        HttpResponse response = httpClient.execute(httpRequest);
        return new HttpSinkResponse(response);
    }

    public String getRequestBody() throws IOException {
        return EntityUtils.toString(httpRequest.getEntity());
    }

    public void addIndex(Integer index) {
        indexes.add(index);
    }

    public ErrorInfo getErrorInfo() {
        return errorInfo;
    }

    public boolean isValid() {
        return valid;
    }

    @NotNull
    @Override
    public Iterator<Integer> iterator() {
        return indexes.iterator();
    }
}
