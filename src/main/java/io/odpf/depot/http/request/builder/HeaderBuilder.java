package io.odpf.depot.http.request.builder;

import io.odpf.depot.common.Template;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.http.enums.HttpParameterSourceType;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import lombok.Getter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.odpf.depot.http.enums.HttpParameterSourceType.KEY;

@Getter
public class HeaderBuilder {

    private final Map<String, String> baseHeaders;
    private final Map<Template, Template> headersTemplate;
    private final HttpParameterSourceType headersParameterSource;
    private final String schemaProtoKeyClass;
    private final String schemaProtoMessageClass;

    public HeaderBuilder(HttpSinkConfig config) {
        this.baseHeaders = config.getSinkHttpHeaders();
        this.headersTemplate = createHeadersTemplateMap(config.getSinkHttpHeadersTemplate());
        this.headersParameterSource = config.getSinkHttpHeadersParameterSource();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    public Map<String, String> build() {
        return baseHeaders;
    }

    public Map<String, String> build(MessageContainer container, OdpfMessageParser odpfMessageParser) throws IOException {
        if (headersParameterSource == KEY) {
            return this.createHeaders(container.getParsedLogKey(odpfMessageParser, schemaProtoKeyClass), odpfMessageParser.getSchema(schemaProtoKeyClass));
        } else {
            return this.createHeaders(container.getParsedLogMessage(odpfMessageParser, schemaProtoMessageClass), odpfMessageParser.getSchema(schemaProtoMessageClass));
        }
    }

    private Map<Template, Template> createHeadersTemplateMap(Properties headersTemplateProperty) {
        if (headersTemplateProperty == null || headersTemplateProperty.isEmpty()) {
            return new HashMap<>();
        }

        return headersTemplateProperty
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        kv -> {
                            try {
                                return new Template(kv.getKey().toString());
                            } catch (InvalidTemplateException e) {
                                throw new IllegalArgumentException(e.getMessage());
                            }
                        },
                        kv -> {
                            try {
                                return new Template(kv.getValue().toString());
                            } catch (InvalidTemplateException e) {
                                throw new IllegalArgumentException(e.getMessage());
                            }
                        }
                ));
    }

    private Map<String, String> createHeaders(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema headersParameterSourceSchema) {
        Map<String, String> headerConfig = new HashMap<>(baseHeaders);
        headersTemplate
                .forEach((k, v) -> {
                    String key = k.parse(parsedOdpfMessage, headersParameterSourceSchema);
                    String value = v.parse(parsedOdpfMessage, headersParameterSourceSchema);
                    headerConfig.put(key, value);
                });
        return headerConfig;
    }
}
