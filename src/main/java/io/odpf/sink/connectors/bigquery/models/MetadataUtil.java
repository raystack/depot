package io.odpf.sink.connectors.bigquery.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import io.odpf.sink.connectors.config.TupleString;

import java.util.List;
import java.util.stream.Collectors;

public class MetadataUtil {
    public static List<Field> getMetadataFields(List<TupleString> metadataColumnsTypes) {
        return metadataColumnsTypes.stream().map(
                tuple -> Field.newBuilder(tuple.getFirst(), LegacySQLTypeName.valueOf(tuple.getSecond()))
                        .setMode(Field.Mode.NULLABLE)
                        .build()).collect(Collectors.toList());
    }

    public static Field getNamespacedMetadataField(String namespace, List<TupleString> metadataColumnsTypes) {
        return Field.newBuilder(namespace, LegacySQLTypeName.RECORD, FieldList.of(getMetadataFields(metadataColumnsTypes)))
                .setMode(Field.Mode.NULLABLE)
                .build();
    }
}
