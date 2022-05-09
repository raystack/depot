package io.odpf.depot.message.json;

import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.expcetion.ConfigurationException;
import io.odpf.depot.expcetion.EmptyMessageException;
import io.odpf.depot.message.InputSchemaMessageMode;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
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
        byte[] payload;
        switch (type) {
            case LOG_KEY:
                payload = (byte[]) message.getLogKey();
                break;
            case LOG_MESSAGE:
                payload = (byte[]) message.getLogMessage();
                break;
            default:
                throw new ConfigurationException("Schema type not supported");
        }
        try {
            if (payload == null || payload.length == 0) {
                log.info("empty message found {}", message.getMetadataString());
                throw new EmptyMessageException();
            }
            return new JsonOdpfParsedMessage(new JSONObject(new String(payload)));
        } catch (JSONException ex) {
            throw new IOException("invalid json error", ex);
        }
    }

    @Override
    public OdpfMessageSchema getSchema(String schemaClass) {
        return null;
    }
}
