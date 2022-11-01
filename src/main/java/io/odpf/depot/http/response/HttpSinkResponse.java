package io.odpf.depot.http.response;

import lombok.Getter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpSinkResponse {

    @Getter
    private final HttpResponse response;
    @Getter
    private final boolean failed;

    public HttpSinkResponse(HttpResponse response, boolean failed) {
        this.response = response;
        this.failed = failed;
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

    public String getStatusLine() {
        return String.valueOf(response.getStatusLine());
    }
}
