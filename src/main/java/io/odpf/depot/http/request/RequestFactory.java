package io.odpf.depot.http.request;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.body.RequestBodyFactory;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.http.enums.HttpRequestType;

public class RequestFactory {

    public static Request create(HttpSinkConfig config) throws InvalidTemplateException {
        UriBuilder uriBuilder = new UriBuilder(config.getSinkHttpServiceUrl());
        HeaderBuilder headerBuilder = new HeaderBuilder(config.getSinkHttpHeaders());
        HttpRequestMethodType httpMethod = config.getSinkHttpRequestMethod();
        RequestBody requestBody = RequestBodyFactory.create(config);

        if (config.getRequestType().equals(HttpRequestType.SINGLE)) {
            return new SingleRequest(httpMethod, headerBuilder, uriBuilder, requestBody);
        } else {
            return new BatchRequest();
        }
    }
}
