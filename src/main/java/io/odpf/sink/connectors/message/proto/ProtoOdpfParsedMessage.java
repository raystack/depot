package io.odpf.sink.connectors.message.proto;

import com.google.protobuf.DynamicMessage;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;

import java.util.Map;
import java.util.Properties;

public class ProtoOdpfParsedMessage implements ParsedOdpfMessage {
    private final DynamicMessage dynamicMessage;

    public ProtoOdpfParsedMessage(DynamicMessage dynamicMessage) {
        this.dynamicMessage = dynamicMessage;
    }

    public String toString() {
        return dynamicMessage.toString();
    }

    @Override
    public Object getRaw() {
        return dynamicMessage;
    }

    @Override
    public Map<String, Object> getMapping() {
        return null;
    }
}
