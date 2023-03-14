package com.gotocompany.depot.message;

import com.gotocompany.depot.config.SinkConfig;

import java.io.IOException;
import java.util.Map;

public interface ParsedMessage {
    Object getRaw();

    void validate(SinkConfig config);

    Map<String, Object> getMapping(MessageSchema schema) throws IOException;

    Object getFieldByName(String name, MessageSchema messageSchema);
}
