package io.odpf.depot.message;

import io.odpf.depot.config.OdpfSinkConfig;

import java.io.IOException;
import java.util.Map;

public interface ParsedOdpfMessage {
    Object getRaw();

    void validate(OdpfSinkConfig config);

    Map<String, Object> getMapping() throws IOException;

    Object getFieldByName(String name, OdpfMessageSchema odpfMessageSchema);
}
