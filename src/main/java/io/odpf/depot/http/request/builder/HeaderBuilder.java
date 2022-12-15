package io.odpf.depot.http.request.builder;

import io.odpf.depot.common.Template;
import io.odpf.depot.common.TemplateUtils;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpParameterSourceType;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.SchemaContainer;
import lombok.Getter;

import java.io.IOException;
import java.util.Map;

@Getter
public class HeaderBuilder {

    private final Map<String, String> baseHeaders;
    private final Map<Template, Template> headersTemplate;
    private final HttpParameterSourceType headersParameterSource;
    private final String schemaProtoKeyClass;
    private final String schemaProtoMessageClass;

    public HeaderBuilder(HttpSinkConfig config) {
        this.baseHeaders = config.getSinkHttpHeaders();
        this.headersTemplate = config.getSinkHttpHeadersTemplate();
        this.headersParameterSource = config.getSinkHttpHeadersParameterSource();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    public Map<String, String> build() {
        return baseHeaders;
    }

    public Map<String, String> build(MessageContainer msgContainer, SchemaContainer schemaContainer) throws IOException {
        Map<String, String> headers;
        if (headersParameterSource == HttpParameterSourceType.KEY) {
            headers = TemplateUtils.parseTemplateMap(
                    headersTemplate,
                    msgContainer.getParsedLogKey(schemaProtoKeyClass),
                    schemaContainer.getSchemaKey(schemaProtoKeyClass)
            );
        } else {
            headers = TemplateUtils.parseTemplateMap(
                    headersTemplate,
                    msgContainer.getParsedLogMessage(schemaProtoMessageClass),
                    schemaContainer.getSchemaMessage(schemaProtoMessageClass)
            );
        }
        baseHeaders.putAll(headers);
        return baseHeaders;
    }
}
