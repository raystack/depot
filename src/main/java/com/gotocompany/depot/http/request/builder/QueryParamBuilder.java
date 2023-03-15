package com.gotocompany.depot.http.request.builder;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.TemplateUtils;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import com.gotocompany.depot.message.MessageContainer;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryParamBuilder {

    private final Map<Template, Template> queryParamTemplates;
    private final HttpParameterSourceType queryParameterSource;
    private final String schemaProtoKeyClass;
    private final String schemaProtoMessageClass;


    public QueryParamBuilder(HttpSinkConfig config) {
        this.queryParamTemplates = config.getQueryTemplate();
        this.queryParameterSource = config.getQueryParamSourceMode();
        this.schemaProtoKeyClass = config.getSinkConnectorSchemaProtoKeyClass();
        this.schemaProtoMessageClass = config.getSinkConnectorSchemaProtoMessageClass();
    }

    public Map<String, String> build() {
        return queryParamTemplates
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        templateKey -> templateKey.getKey().getTemplateString(),
                        templateValue -> templateValue.getValue().getTemplateString()
                ));
    }

    public Map<String, String> build(MessageContainer msgContainer) throws IOException {
        if (queryParameterSource == HttpParameterSourceType.KEY) {
            return TemplateUtils.parseTemplateMap(
                    queryParamTemplates,
                    msgContainer.getParsedLogKey(schemaProtoKeyClass)
            );
        } else {
            return TemplateUtils.parseTemplateMap(
                    queryParamTemplates,
                    msgContainer.getParsedLogMessage(schemaProtoMessageClass)
            );
        }
    }
}
