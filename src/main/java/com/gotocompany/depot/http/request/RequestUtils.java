package com.gotocompany.depot.http.request;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.net.URI;
import java.util.Map;

@Slf4j
public class RequestUtils {

    protected static HttpRequestRecord createErrorRecord(Exception e, ErrorType type, Integer index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        HttpRequestRecord record = new HttpRequestRecord(errorInfo);
        record.addIndex(index);
        return record;
    }

    private static StringEntity buildStringEntity(Object input) {
        return new StringEntity(input.toString(), ContentType.APPLICATION_JSON);
    }

    public static HttpEntityEnclosingRequestBase buildRequest(HttpRequestMethodType requestMethod, Map<String, String> headers, URI uri, Object requestBodies) {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, requestMethod);
        headers.forEach(request::addHeader);
        request.setEntity(buildStringEntity(requestBodies));
        return request;
    }
}
