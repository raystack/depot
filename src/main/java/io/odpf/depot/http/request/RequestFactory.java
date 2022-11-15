package io.odpf.depot.http.request;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.body.RequestBodyFactory;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.http.enums.HttpRequestType;
import org.apache.commons.lang3.StringUtils;

public class RequestFactory {

    public static Request create(HttpSinkConfig config) {
        String serviceUrl = config.getSinkHttpServiceUrl();
        validateUrlConfig(serviceUrl);
        UriBuilder uriBuilder = new UriBuilder(serviceUrl);
        HeaderBuilder headerBuilder = new HeaderBuilder(config.getSinkHttpHeaders());
        HttpRequestMethodType httpMethod = config.getSinkHttpRequestMethod();
        RequestBody requestBody = RequestBodyFactory.create(config);

        if (config.getRequestType().equals(HttpRequestType.SINGLE)) {
            return new SingleRequest(httpMethod, headerBuilder, uriBuilder, requestBody);
        } else {
            return new BatchRequest(httpMethod, headerBuilder, uriBuilder, requestBody);
        }
    }

    private static void validateUrlConfig(String serviceUrl) {
        if (serviceUrl == null || StringUtils.isEmpty(serviceUrl)) {
            throw new ConfigurationException("Service URL '" + serviceUrl + "' is invalid");
        }
    }
}
