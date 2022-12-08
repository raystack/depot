package io.odpf.depot.message;

import java.io.IOException;

public class MessageContainer {
    private final OdpfMessage message;
    private ParsedOdpfMessage parsedLogKey = null;
    private ParsedOdpfMessage parsedLogMessage = null;

    public MessageContainer(OdpfMessage message) {
        this.message = message;
    }

    public ParsedOdpfMessage getParsedLogKey(OdpfMessageParser parser, String schemaProtoKeyClass) throws IOException {
        if (parsedLogKey == null) {
            parsedLogKey = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, schemaProtoKeyClass);
        }
        return parsedLogKey;
    }

    public ParsedOdpfMessage getParsedLogMessage(OdpfMessageParser parser, String schemaProtoMessageClass) throws IOException {
        if (parsedLogMessage == null) {
            parsedLogMessage = parser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaProtoMessageClass);
        }
        return parsedLogMessage;
    }
}
