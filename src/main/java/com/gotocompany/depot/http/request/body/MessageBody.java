package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;

public class MessageBody implements RequestBody {

    private final HttpSinkConfig config;

    public MessageBody(HttpSinkConfig config) {
        this.config = config;
    }

    @Override
    public String build(MessageContainer messageContainer) throws IOException {
        ParsedMessage parsedMessage;
        if (config.getSinkConnectorSchemaMessageMode() == SinkConnectorSchemaMessageMode.LOG_KEY) {
            parsedMessage = messageContainer.getParsedLogKey(config.getSinkConnectorSchemaProtoKeyClass());
        } else {
            parsedMessage = messageContainer.getParsedLogMessage(config.getSinkConnectorSchemaProtoMessageClass());
        }
        return parsedMessage.toJson().toString();
    }
}
