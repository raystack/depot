package io.odpf.sink.connectors.message;

import io.odpf.sink.connectors.config.OdpfSinkConfig;
import org.json.JSONObject;

import java.io.IOException;

public class JsonOdpfMessageParser implements OdpfMessageParser {

    private final OdpfSinkConfig config;

    public JsonOdpfMessageParser(OdpfSinkConfig config) {
        this.config = config;
    }

    @Override
    public ParsedOdpfMessage parse(OdpfMessage message, InputSchemaMessageMode type) throws IOException {
        switch (type) {
            case LOG_KEY:
                return new JsonOdpfParsedMessage(new JSONObject(new String(message.getLogKey())));
            case LOG_MESSAGE:
                return new JsonOdpfParsedMessage(new JSONObject(new String(message.getLogMessage())));
            default:
                throw new IOException("Error while parsing Message");
        }
    }
}
