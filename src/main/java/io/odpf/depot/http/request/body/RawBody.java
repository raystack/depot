package io.odpf.depot.http.request.body;

import com.google.gson.JsonObject;
import io.odpf.depot.common.TupleString;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.message.OdpfMessage;

import java.util.List;
import java.util.Map;
import java.util.Base64;

public class RawBody implements RequestBody {
    private final HttpSinkConfig config;

    public RawBody(HttpSinkConfig config) {
        this.config = config;
    }

    @Override
    public String build(OdpfMessage message) {
        JsonObject payload = new JsonObject();
        List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
        Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
        metadata.forEach(
                (k, v) -> payload.addProperty(k, v.toString())
        );
        String logKey = Base64.getEncoder().encodeToString((byte[]) message.getLogKey());
        String logMessage = Base64.getEncoder().encodeToString((byte[]) message.getLogMessage());
        payload.addProperty("log_key", logKey);
        payload.addProperty("log_message", logMessage);
        return payload.toString();
    }
}
