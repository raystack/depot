package io.odpf.depot.http.request;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpParameterSourceType;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.enums.HttpRequestType;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.body.RequestBodyFactory;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.redis.parsers.Template;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class RequestFactory {

    public static Request create(HttpSinkConfig config, OdpfMessageParser odpfMessageParser) throws IOException {
        Map<Template, Template> headersTemplate = config.getSinkHttpHeadersTemplate()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        kv -> new Template(kv.getKey().toString()),
                        kv -> new Template(kv.getValue().toString())
                ));
        SinkConnectorSchemaMessageMode headersParameterSource = config.getSinkHttpHeadersParameterSource() == HttpParameterSourceType.MESSAGE
                ? SinkConnectorSchemaMessageMode.LOG_MESSAGE : SinkConnectorSchemaMessageMode.LOG_KEY;

        String headersParameterSourceSchemaClass = headersParameterSource == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();

        OdpfMessageSchema headersParameterSourceSchema = odpfMessageParser.getSchema(headersParameterSourceSchemaClass);
        HeaderBuilder headerBuilder = new HeaderBuilder(
                odpfMessageParser,
                config.getSinkHttpHeaders(),
                headersTemplate,
                headersParameterSource,
                headersParameterSourceSchemaClass,
                headersParameterSourceSchema
        );

        UriBuilder uriBuilder = new UriBuilder(config.getSinkHttpServiceUrl());
        HttpRequestMethodType httpMethod = config.getSinkHttpRequestMethod();
        RequestBody requestBody = RequestBodyFactory.create(config);

        if (config.getRequestType().equals(HttpRequestType.SINGLE)) {
            return new SingleRequest(httpMethod, headerBuilder, uriBuilder, requestBody);
        } else {
            return new BatchRequest();
        }
    }
}
