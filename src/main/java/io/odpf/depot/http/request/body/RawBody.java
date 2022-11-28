package io.odpf.depot.http.request.body;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.InvalidMessageException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.MessageUtils;
import org.json.JSONObject;

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
        payload.put("log_key", encodedSerializedStringFrom(message.getLogKey()));
        payload.put("log_message", encodedSerializedStringFrom(message.getLogMessage()));
        MessageUtils.getMetaData(message, config, Date::new).forEach(payload::put);
        return payload.toString();
    }

    private String a(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }

    private String encodedSerializedStringFrom(Object logMessage) {
        if (logMessage instanceof byte[]) {
            return new String(Base64.getEncoder().encode((byte[]) logMessage));
        } else if (logMessage == null) {
            return new String(Base64.getEncoder().encode(new byte[0]));
        } else {
            throw new InvalidMessageException("Could not encode the key or message. Key or message should be in bytes");
        }
    }
}
