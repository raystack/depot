package io.odpf.depot.message;

import java.io.IOException;

public class MessageContainer {
    private final OdpfMessage message;

    private final OdpfMessageParser parser;
    private ParsedOdpfMessage parsedLogKey = null;
    private ParsedOdpfMessage parsedLogMessage = null;

    public MessageContainer(OdpfMessage message, OdpfMessageParser parser) {
        this.message = message;
        this.parser = parser;
    }

    public ParsedOdpfMessage getParsedLogKey(String schemaProtoKeyClass) throws IOException {
        if (parsedLogKey == null) {
            parsedLogKey = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, schemaProtoKeyClass);
        }
        return parsedLogKey;
    }

    public ParsedOdpfMessage getParsedLogMessage(String schemaProtoMessageClass) throws IOException {
        if (parsedLogMessage == null) {
            parsedLogMessage = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaProtoMessageClass);
        }
        return parsedLogMessage;
    }
}
