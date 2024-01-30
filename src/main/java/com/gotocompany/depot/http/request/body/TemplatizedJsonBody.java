package com.gotocompany.depot.http.request.body;

import com.fasterxml.jackson.databind.JsonNode;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.request.util.JsonParserUtils;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;

public class TemplatizedJsonBody implements RequestBody {
    private final JsonNode templateJsonNode;
    private final HttpSinkConfig config;

    public TemplatizedJsonBody(HttpSinkConfig config) {
        this.config = config;
        this.templateJsonNode = JsonParserUtils.createJsonNode(config.getSinkHttpJsonBodyTemplate());
    }

    @Override
    public String build(MessageContainer msgContainer) throws IOException {
        ParsedMessage parsedMessage;
        if (config.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_KEY) {
            parsedMessage = msgContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        } else {
            parsedMessage = msgContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        }
        JsonNode parsedJsonNode = JsonParserUtils.parse(templateJsonNode, parsedMessage);
        return parsedJsonNode.toString();
    }
}
