package org.raystack.depot.bigquery.storage.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.raystack.depot.common.TupleString;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.message.proto.converter.fields.ProtoField;
import org.raystack.depot.message.proto.converter.fields.ProtoFieldFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BigQueryProtoUtils {
    public static void addMetadata(
            Map<String, Object> metadata,
            DynamicMessage.Builder messageBuilder,
            Descriptors.Descriptor tableDescriptor,
            BigQuerySinkConfig config) {
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            if (config.getBqMetadataNamespace().isEmpty()) {
                setMetadata(metadata, messageBuilder, tableDescriptor, metadataColumnsTypes);
            } else {
                String namespace = config.getBqMetadataNamespace();
                Descriptors.FieldDescriptor metadataFieldDescriptor = tableDescriptor.findFieldByName(namespace);
                if (metadataFieldDescriptor != null) {
                    Descriptors.Descriptor metadataDescriptor = metadataFieldDescriptor.getMessageType();
                    DynamicMessage.Builder metadataBuilder = DynamicMessage.newBuilder(metadataDescriptor);
                    setMetadata(metadata, metadataBuilder, metadataDescriptor, metadataColumnsTypes);
                    messageBuilder.setField(metadataFieldDescriptor, metadataBuilder.build());
                }
            }
        }
    }

    private static void setMetadata(Map<String, Object> metadata,
            DynamicMessage.Builder messageBuilder,
            Descriptors.Descriptor descriptor,
            List<TupleString> metadataColumnsTypes) {
        metadataColumnsTypes.forEach(tuple -> {
            String name = tuple.getFirst();
            String type = tuple.getSecond();
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(name);
            if (field != null && metadata.get(name) != null) {
                ProtoField protoField = ProtoFieldFactory.getField(field, metadata.get(name));
                Object fieldValue = protoField.getValue();
                if ("timestamp".equals(type) && fieldValue instanceof Long) {
                    fieldValue = TimeUnit.MILLISECONDS.toMicros((Long) fieldValue);
                }
                messageBuilder.setField(field, fieldValue);
            }
        });
    }
}
