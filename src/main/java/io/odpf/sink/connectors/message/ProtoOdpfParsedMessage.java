package io.odpf.sink.connectors.message;

import com.google.protobuf.DynamicMessage;

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
}
