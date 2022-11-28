package io.odpf.depot.http.request.body;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.InvalidMessageException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.MessageUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Base64;
import java.util.Date;

public class RawBody implements RequestBody {
    private final HttpSinkConfig config;

    public RawBody(HttpSinkConfig config) {
        this.config = config;
    }

    @Override
    public String build(OdpfMessage message) {
        JSONObject payload = new JSONObject();
        try {
            MessageUtils.validate(message, byte[].class);
            payload.put("log_key", encodedSerializedStringFrom((byte[]) message.getLogKey()));
            payload.put("log_message", encodedSerializedStringFrom((byte[]) message.getLogMessage()));
            MessageUtils.getMetaData(message, config, Date::new).forEach(payload::put);
            return payload.toString();
        } catch (IOException e) {
            throw new InvalidMessageException("Could not encode the key or message. Key or message should be in bytes");
        }
    }

    private String encodedSerializedStringFrom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }
}
