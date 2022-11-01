package io.odpf.depot.http.request.body;

import com.google.gson.JsonObject;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.message.OdpfMessage;

import java.util.Base64;
import java.util.Map;

public class RawBody implements RequestBody {
    private final HttpSinkConfig config;

    public RawBody(HttpSinkConfig config) {
        this.config = config;
    }

    @Override
    public String build(OdpfMessage message) {
        JsonObject payload = new JsonObject();
        if (config.shouldAddMetadata()) {
            Map<String, Object> metadata = message.getMetadata(config.getMetadataColumnsTypes());
            metadata.forEach(
                    (k, v) -> payload.addProperty(k, v.toString())
            );
        }

        payload.addProperty("log_key", encodedSerializedStringFrom((byte[]) message.getLogKey()));
        payload.addProperty("log_message", encodedSerializedStringFrom((byte[]) message.getLogMessage()));
        return payload.toString();
    }

    private String encodedSerializedStringFrom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(Base64.getEncoder().encode(bytes));
    }
}
