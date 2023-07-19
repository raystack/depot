package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.TemplateUtils;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import com.gotocompany.depot.message.MessageContainer;
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
        if (!headersTemplate.isEmpty()) {
            throw new ConfigurationException("Header template is not allowed in batch request mode.");
        }
        return baseHeaders;
    }

    public Map<String, String> build(MessageContainer msgContainer) throws IOException {
        Map<String, String> headers;
        if (headersParameterSource == HttpParameterSourceType.KEY) {
            headers = TemplateUtils.parseTemplateMap(
                    headersTemplate,
                    msgContainer.getParsedLogKey(schemaProtoKeyClass)
            );
        } else {
            headers = TemplateUtils.parseTemplateMap(
                    headersTemplate,
                    msgContainer.getParsedLogMessage(schemaProtoMessageClass)
            );
        }
        baseHeaders.putAll(headers);
        return baseHeaders;
    }
}
