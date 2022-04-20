package io.odpf.sink.connectors.bigquery.converter;

import com.google.protobuf.Descriptors;
import io.odpf.sink.connectors.bigquery.converter.fields.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ProtoFieldFactory {

    public static ProtoField getField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        List<ProtoField> protoFields = Arrays.asList(
                new TimestampProtoField(descriptor, fieldValue),
                new EnumProtoField(descriptor, fieldValue),
                new ByteProtoField(descriptor, fieldValue),
                new StructProtoField(descriptor, fieldValue),
                new NestedProtoField(descriptor, fieldValue)
        );
        Optional<ProtoField> first = protoFields
                .stream()
                .filter(ProtoField::matches)
                .findFirst();
        return first.orElseGet(() -> new DefaultProtoField(fieldValue));
    }

}
