package io.odpf.depot.http.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.http.response.HttpSinkResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.regex.Pattern;

@AllArgsConstructor
@Getter
public class HttpRequestRecord {

    protected static final String SUCCESS_CODE_PATTERN = "^2.*";

    private HttpEntityEnclosingRequestBase httpRequest;
    private final Long index;
    private final ErrorInfo errorInfo;
    private final boolean valid;

    public HttpSinkResponse send(HttpClient httpClient) throws IOException {
        HttpResponse response = httpClient.execute(this.httpRequest);
        return addSinkResponse(response);
    }

    private HttpSinkResponse addSinkResponse(HttpResponse response) {
        consumeResponse(response);
        boolean isSuccess = Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches();
        if (isSuccess) {
            return new HttpSinkResponse(response, false);
        } else {
            return new HttpSinkResponse(response, true);
        }
    }

    private void consumeResponse(HttpResponse response) {
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }
}
