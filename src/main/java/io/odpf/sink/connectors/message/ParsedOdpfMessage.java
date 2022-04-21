package io.odpf.sink.connectors.message;

import java.util.Map;

public interface ParsedOdpfMessage {
    Object getRaw();

    Map<String, Object> getMapping();
}