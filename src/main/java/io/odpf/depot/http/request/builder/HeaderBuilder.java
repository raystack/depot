package io.odpf.depot.http.request.builder;

import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.redis.parsers.Template;

import java.io.IOException;
import java.util.Map;

public class HeaderBuilder {

    private final OdpfMessageParser odpfMessageParser;
    private final Map<String, String> headerConfig;
    private final Map<Template, Template> headersTemplate;
    private final SinkConnectorSchemaMessageMode headersParameterSource;
    private final String headersParameterSourceSchemaClass;
    private final OdpfMessageSchema headersParameterSourceSchema;

    public HeaderBuilder(OdpfMessageParser odpfMessageParser, Map<String, String> baseHeaders, Map<Template, Template> headersTemplate, SinkConnectorSchemaMessageMode headersParameterSource, String headersParameterSourceSchemaClass, OdpfMessageSchema headersParameterSourceSchema) {
        this.odpfMessageParser = odpfMessageParser;
        this.headerConfig = baseHeaders;
        this.headersTemplate = headersTemplate;
        this.headersParameterSource = headersParameterSource;
        this.headersParameterSourceSchemaClass = headersParameterSourceSchemaClass;
        this.headersParameterSourceSchema = headersParameterSourceSchema;
    }

    public Map<String, String> build() {
        return headerConfig;
    }

    public Map<String, String> build(OdpfMessage message) throws IOException {
        if (headersTemplate == null || headersTemplate.size() == 0) {
            return headerConfig;
        }
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, headersParameterSource, headersParameterSourceSchemaClass);
        headersTemplate
                .forEach((k, v) -> {
                    String key = k.parse(parsedOdpfMessage, headersParameterSourceSchema);
                    String value = v.parse(parsedOdpfMessage, headersParameterSourceSchema);
                    headerConfig.put(key, value);
                });
        return headerConfig;
    }
}
