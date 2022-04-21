package io.odpf.sink.connectors.message.json;

import io.odpf.sink.connectors.config.Tuple;
import io.odpf.sink.connectors.message.OdpfMessage;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class JsonOdpfMessage implements OdpfMessage {
    private final byte[] logMessage;
    private final byte[] logKey;
    private final Map<String, Object> metadata = new HashMap<>();

    @Override
    public String getMetadataString() {
        return metadata.keySet().stream()
                .map(key -> key + "=" + metadata.get(key))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    @Override
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @SafeVarargs
    public JsonOdpfMessage(byte[] logKey, byte[] logMessage, Tuple<String, Object>... tuples) {
        this.logKey = logKey;
        this.logMessage = logMessage;
        Arrays.stream(tuples).forEach(t -> metadata.put(t.getFirst(), t.getSecond()));
    }
}
