package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageUtils;
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
    public String build(MessageContainer messageContainer) throws IOException {
        Message message = messageContainer.getMessage();
        JSONObject payload = new JSONObject();
        MessageUtils.validate(message, byte[].class);
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
