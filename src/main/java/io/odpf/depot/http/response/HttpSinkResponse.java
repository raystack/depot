package io.odpf.depot.http.response;

import lombok.Getter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.regex.Pattern;

public class HttpSinkResponse {

    protected static final String SUCCESS_CODE_PATTERN = "^2.*";
    @Getter
    private final HttpResponse response;
    @Getter
    private final boolean failed;

    protected HttpSinkResponse(HttpResponse response, boolean failed) {
        this.response = response;
        this.failed = failed;
    }

    public HttpSinkResponse(HttpResponse response) {
        this(response, !Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches());
    }

    public String getResponseCode() {
        if (response != null && response.getStatusLine() != null) {
            return Integer.toString(response.getStatusLine().getStatusCode());
        } else {
            return "null";
        }
    }

    public String getResponseBody() throws IOException {
        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity);
    }
}
