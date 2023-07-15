package org.raystack.depot.message.proto.converter.fields;

public interface ProtoField {

    Object getValue();

    boolean matches();
}
