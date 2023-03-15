package com.gotocompany.depot.message;

import java.io.IOException;

public class MessageContainer {
    private final Message message;

    private final MessageParser parser;
    private ParsedMessage parsedLogKey = null;
    private ParsedMessage parsedLogMessage = null;

    public MessageContainer(Message message, MessageParser parser) {
        this.message = message;
        this.parser = parser;
    }

    public ParsedMessage getParsedLogKey(String schemaProtoKeyClass) throws IOException {
        if (parsedLogKey == null) {
            parsedLogKey = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, schemaProtoKeyClass);
        }
        return parsedLogKey;
    }

    public ParsedMessage getParsedLogMessage(String schemaProtoMessageClass) throws IOException {
        if (parsedLogMessage == null) {
            parsedLogMessage = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaProtoMessageClass);
        }
        return parsedLogMessage;
    }

    public Message getMessage() {
        return message;
    }
}
