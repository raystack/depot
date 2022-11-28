package io.odpf.depot.http.response;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.regex.Pattern;

public class HttpSinkResponse {

    protected static final String SUCCESS_CODE_PATTERN = "^2.*";
    private final HttpResponse response;

    public HttpSinkResponse(HttpResponse response) {
        this.response = response;
    }

    public boolean isFailed() {
        if (response != null && response.getStatusLine() != null) {
            return !Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches();
        } else {
            return true;
        }
    }

    public String getResponseCode() {
        if (response != null && response.getStatusLine() != null) {
            return Integer.toString(response.getStatusLine().getStatusCode());
        } else {
            return "null";
        }
    }

    public String getResponseBody() {
        HttpEntity entity = response.getEntity();
        try {
            return EntityUtils.toString(entity);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
