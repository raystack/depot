package io.odpf.sink.connectors.message.json;

import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.expcetion.EmptyMessageException;
import io.odpf.sink.connectors.message.InputSchemaMessageMode;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.message.OdpfMessageSchema;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

@Slf4j
public class JsonOdpfMessageParser implements OdpfMessageParser {

    private final OdpfSinkConfig config;

    public JsonOdpfMessageParser(OdpfSinkConfig config) {
        this.config = config;
    }

    @Override
    public ParsedOdpfMessage parse(OdpfMessage message, InputSchemaMessageMode type, String schemaClass) throws IOException {
        if (type == null) {
            throw new IOException("message mode not defined");
        }
        switch (type) {
            case LOG_KEY:
                try {
                    if (message.getLogKey() == null || message.getLogKey().length == 0) {
                        log.info("empty message found {}", message.getMetadataString());
                        throw new EmptyMessageException();
                    }
                    return new JsonOdpfParsedMessage(new JSONObject(new String(message.getLogKey())));
                } catch (JSONException ex) {
                    throw new IOException("invalid json error", ex);
                }
            case LOG_MESSAGE:
                try {
                    if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
                        log.info("empty message found {}", message.getMetadataString());
                        throw new EmptyMessageException();
                    }
                    return new JsonOdpfParsedMessage(new JSONObject(new String(message.getLogMessage())));
                } catch (JSONException ex) {
                    throw new IOException("invalid json error", ex);
                }
            default:
                throw new IOException("Error while parsing Message");
        }
    }

    @Override
    public OdpfMessageSchema getSchema(String schemaClass) {
        return null;
    }
}
