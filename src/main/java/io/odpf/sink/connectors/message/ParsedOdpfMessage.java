package io.odpf.sink.connectors.message;

import io.odpf.sink.connectors.config.OdpfSinkConfig;

import java.io.IOException;
import java.util.Map;

public interface ParsedOdpfMessage {
    Object getRaw();

    void validate(OdpfSinkConfig config);

    Map<String, Object> getMapping(OdpfMessageSchema schema) throws IOException;
}
