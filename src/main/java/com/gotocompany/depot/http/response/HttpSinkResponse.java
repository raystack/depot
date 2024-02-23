package com.gotocompany.depot.http.response;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.regex.Pattern;

public class HttpSinkResponse {

    protected static final String SUCCESS_CODE_PATTERN = "^2.*";
    private boolean isFail;
    private String responseCode;
    private String responseBody;

    public HttpSinkResponse(HttpResponse response) throws IOException {
        setIsFail(response);
        setResponseCode(response);
        setResponseBody(response);
    }

    private void setIsFail(HttpResponse response) {
        if (response != null && response.getStatusLine() != null) {
            isFail = Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches();
        } else {
            isFail = true;
        }
    }

    private void setResponseCode(HttpResponse response) {
        if (response != null && response.getStatusLine() != null) {
            responseCode = Integer.toString(response.getStatusLine().getStatusCode());
        } else {
            responseCode = "null";
        }
    }

    private void setResponseBody(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        responseBody = EntityUtils.toString(entity);
    }

    public String getResponseBody() {
        return responseBody;
    }

    public String getResponseCode() {
        return responseCode;
    }

    public boolean isFail() {
        return isFail;
    }
}
