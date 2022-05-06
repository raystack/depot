package io.odpf.depot.message.json;

import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
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
        if (!config.getSinkConnectorSchemaJsonOutputDefaultDatatypeStringEnable()) {
            throw new UnsupportedOperationException("currently only string data type for values is supported");
        }
        this.config = config;

    }


    @Override
    public ParsedOdpfMessage parse(OdpfMessage message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException {
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
            //TODO check if this can be done as part of constructor
            JSONObject jsonObject = new JSONObject(new String(payload));
            JSONObject jsonWithStringValues = new JSONObject();
            jsonObject.keySet()
                    .forEach(k -> {
                        Object value = jsonObject.get(k);
                        if (value instanceof JSONObject) {
                            throw new UnsupportedOperationException("nested json structure not supported yet");
                        }
                        if (JSONObject.NULL.equals(value)) {
                            return;
                        }
                        jsonWithStringValues.put(k, value.toString());
                    });
            return new JsonOdpfParsedMessage(jsonWithStringValues);
        } catch (JSONException ex) {
            throw new IOException("invalid json error", ex);
        }
    }

    @Override
    public OdpfMessageSchema getSchema(String schemaClass) {
        return null;
    }
}
