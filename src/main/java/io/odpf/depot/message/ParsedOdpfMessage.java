package org.raystack.depot.message;

import org.raystack.depot.config.RaystackSinkConfig;

import java.io.IOException;
import java.util.Map;

public interface ParsedRaystackMessage {
    Object getRaw();

    void validate(RaystackSinkConfig config);

    Map<String, Object> getMapping(RaystackMessageSchema schema) throws IOException;

    Object getFieldByName(String name, RaystackMessageSchema raystackMessageSchema);
}
