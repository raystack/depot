package io.odpf.depot.http.request;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.enums.HttpRequestType;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.body.RequestBodyFactory;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.OdpfMessageParser;

public class RequestFactory {

    public static Request create(HttpSinkConfig config, OdpfMessageParser odpfMessageParser) throws InvalidTemplateException {
        HeaderBuilder headerBuilder = new HeaderBuilder(config);
        UriBuilder uriBuilder = new UriBuilder(config.getSinkHttpServiceUrl());
        HttpRequestMethodType httpRequestMethod = config.getSinkHttpRequestMethod();
        RequestBody requestBody = RequestBodyFactory.create(config);

        if (config.getRequestType().equals(HttpRequestType.SINGLE)) {
            return new SingleRequest(httpRequestMethod, headerBuilder, uriBuilder, requestBody, odpfMessageParser);
        } else {
            return new BatchRequest(httpMethod, headerBuilder, uriBuilder, requestBody);
        }
    }
}
