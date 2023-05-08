package com.gotocompany.depot.http.request;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.enums.HttpRequestType;
import com.gotocompany.depot.http.request.builder.HeaderBuilder;
import com.gotocompany.depot.http.request.builder.QueryParamBuilder;
import com.gotocompany.depot.http.request.builder.UriBuilder;
import com.gotocompany.depot.message.MessageParser;

public class RequestFactory {

    public static Request create(HttpSinkConfig config, MessageParser messageParser) throws InvalidTemplateException {
        HeaderBuilder headerBuilder = new HeaderBuilder(config);
        QueryParamBuilder queryParamBuilder = new QueryParamBuilder(config);
        UriBuilder uriBuilder = new UriBuilder(config);

        if (config.getRequestType().equals(HttpRequestType.SINGLE)) {
            return new SingleRequest(headerBuilder, queryParamBuilder, uriBuilder, config, messageParser);
        } else {
            return new BatchRequest(headerBuilder, queryParamBuilder, uriBuilder, config, messageParser);
        }
    }
}
