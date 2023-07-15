package org.raystack.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ProtoFieldFactory {

    public static ProtoField getField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        List<ProtoField> protoFields = Arrays.asList(
                new DurationProtoField(descriptor, fieldValue),
                new TimestampProtoField(descriptor, fieldValue),
                new EnumProtoField(descriptor, fieldValue),
                new StructProtoField(descriptor, fieldValue),
                new FloatProtoField(descriptor, fieldValue),
                new IntegerProtoField(descriptor, fieldValue),
                new MessageProtoField(descriptor, fieldValue));
        Optional<ProtoField> first = protoFields
                .stream()
                .filter(ProtoField::matches)
                .findFirst();
        return first.orElseGet(() -> new DefaultProtoField(fieldValue));
    }
}
