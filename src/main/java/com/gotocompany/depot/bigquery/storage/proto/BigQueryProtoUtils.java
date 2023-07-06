package com.gotocompany.depot.bigquery.storage.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;

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
                Object fieldValue = metadata.get(name);
                if ("timestamp".equals(type) && fieldValue instanceof Long) {
                    fieldValue = TimeUnit.MILLISECONDS.toMicros((Long) fieldValue);
                }
                if ("integer".equals(type)) {
                    fieldValue = Long.valueOf(fieldValue.toString());
                }
                messageBuilder.setField(field, fieldValue);
            }
        });
    }
}
