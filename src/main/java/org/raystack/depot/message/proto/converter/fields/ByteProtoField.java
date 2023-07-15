package org.raystack.depot.message.proto.converter.fields;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

import java.util.Base64;
import java.util.Collection;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ByteProtoField implements ProtoField {

    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        if (fieldValue instanceof Collection<?>) {
            return ((Collection<?>) fieldValue).stream().map(this::getByteString).collect(Collectors.toList());
        }
        return getByteString(fieldValue);
    }

    private Object getByteString(Object field) {
        ByteString byteString = (ByteString) field;
        byte[] bytes = byteString.toStringUtf8().getBytes();
        return base64Encode(bytes);
    }

    private String base64Encode(byte[] bytes) {
        return new String(Base64.getEncoder().encode(bytes));
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.BYTES;
    }
}
