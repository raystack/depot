package io.odpf.depot.http.request.body;

import io.odpf.depot.config.HttpSinkConfig;
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
        payload.put("log_key", encodedSerializedStringFrom((byte[]) message.getLogKey()));
        payload.put("log_message", encodedSerializedStringFrom((byte[]) message.getLogMessage()));
        MessageUtils.getMetaData(message, config, Date::new).forEach(payload::put);
        return payload.toString();
    }

    private String encodedSerializedStringFrom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }
}
