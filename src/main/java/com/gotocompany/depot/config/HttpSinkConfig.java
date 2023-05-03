package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.HttpHeaderConverter;
import com.gotocompany.depot.config.converter.HttpParameterSourceTypeConverter;
import com.gotocompany.depot.config.converter.HttpRequestBodyTypeConverter;
import com.gotocompany.depot.config.converter.HttpRequestMethodConverter;
import com.gotocompany.depot.config.converter.HttpRequestTypeConverter;
import com.gotocompany.depot.config.converter.RangeToHashMapConverter;
import com.gotocompany.depot.config.converter.TemplateMapConverter;
import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.enums.HttpRequestType;
import com.gotocompany.depot.common.Template;
import org.aeonbits.owner.Config;

import java.util.Map;


@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface HttpSinkConfig extends HttpClientConfig {

    @Key("SINK_HTTP_SERVICE_URL")
    String getSinkHttpServiceUrl();

    @Key("SINK_HTTP_REQUEST_METHOD")
    @DefaultValue("put")
    @ConverterClass(HttpRequestMethodConverter.class)
    HttpRequestMethodType getSinkHttpRequestMethod();

    @Key("SINK_HTTP_HEADERS")
    @DefaultValue("")
    @ConverterClass(HttpHeaderConverter.class)
    Map<String, String> getSinkHttpHeaders();

    @Key("SINK_HTTP_HEADERS_TEMPLATE")
    @DefaultValue("")
    @ConverterClass(TemplateMapConverter.class)
    Map<Template, Template> getSinkHttpHeadersTemplate();

    @Key("SINK_HTTP_HEADERS_PARAMETER_SOURCE")
    @DefaultValue("MESSAGE")
    @ConverterClass(HttpParameterSourceTypeConverter.class)
    HttpParameterSourceType getSinkHttpHeadersParameterSource();

    @Key("SINK_HTTP_QUERY_TEMPLATE")
    @DefaultValue("")
    @ConverterClass(TemplateMapConverter.class)
    Map<Template, Template> getQueryTemplate();

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

    @Key("SINK_HTTP_REQUEST_LOG_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRequestLogStatusCodeRanges();

    @Key("SINK_HTTP_RETRY_STATUS_CODE_RANGES")
    @DefaultValue("400-600")
    @ConverterClass(RangeToHashMapConverter.class)
    Map<Integer, Boolean> getSinkHttpRetryStatusCodeRanges();

    @Key("SINK_HTTP_JSON_BODY_TEMPLATE")
    @DefaultValue("")
    String getSinkHttpJsonBodyTemplate();

    @Key("SINK_HTTP_DATE_FORMAT_ENABLE")
    @DefaultValue("false")
    Boolean isSinkHttpDateFormatEnabled();
}
