package io.odpf.sink.connectors.message.proto.converter.fields;

public interface ProtoField {

    Object getValue();

    boolean matches();
}
