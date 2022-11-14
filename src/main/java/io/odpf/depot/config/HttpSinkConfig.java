package io.odpf.depot.config;

import io.odpf.depot.config.converter.HttpParameterSourceTypeConverter;
import io.odpf.depot.config.converter.HttpRequestBodyTypeConverter;
import io.odpf.depot.config.converter.HttpRequestMethodConverter;
import io.odpf.depot.config.converter.HttpRequestTypeConverter;
import io.odpf.depot.config.converter.JsonToPropertiesConverter;
import io.odpf.depot.http.enums.HttpParameterSourceType;
import io.odpf.depot.http.enums.HttpRequestBodyType;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.enums.HttpRequestType;
import org.aeonbits.owner.Config;

import java.util.Properties;


@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface HttpSinkConfig extends HttpClientConfig, OdpfSinkConfig {

    @Key("SINK_HTTP_SERVICE_URL")
    String getSinkHttpServiceUrl();

    @Key("SINK_HTTP_REQUEST_METHOD")
    @DefaultValue("put")
    @ConverterClass(HttpRequestMethodConverter.class)
    HttpRequestMethodType getSinkHttpRequestMethod();

    @Key("SINK_HTTP_HEADERS")
    @DefaultValue("")
    String getSinkHttpHeaders();

    @Key("SINK_HTTP_HEADERS_TEMPLATE")
    @DefaultValue("")
    @ConverterClass(JsonToPropertiesConverter.class)
    Properties getHeaderTemplate();

    @Key("SINK_HTTP_HEADERS_PARAMETER_SOURCE")
    @DefaultValue("MESSAGE")
    @ConverterClass(HttpParameterSourceTypeConverter.class)
    HttpParameterSourceType getHeaderSourceMode();

    @Key("SINK_HTTP_QUERY_PARAMETER_SOURCE")
    @DefaultValue("MESSAGE")
    @ConverterClass(HttpParameterSourceTypeConverter.class)
    HttpParameterSourceType getQueryParamSourceMode();

    @Key("SINK_HTTP_REQUEST_MODE")
    @DefaultValue("SINGLE")
    @ConverterClass(HttpRequestTypeConverter.class)
    HttpRequestType getRequestType();

    @Key("SINK_HTTP_REQUEST_BODY_MODE")
    @DefaultValue("RAW")
    @ConverterClass(HttpRequestBodyTypeConverter.class)
    HttpRequestBodyType getRequestBodyType();

}
