package io.odpf.sink.connectors.bigquery.proto;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import io.odpf.sink.connectors.bigquery.models.BQField;
import io.odpf.sink.connectors.common.TupleString;
import io.odpf.sink.connectors.message.proto.ProtoField;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoUtils {
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

    public static List<Field> generateBigquerySchema(ProtoField protoField) {
        if (protoField == null) {
            return null;
        }
        List<Field> schemaFields = new ArrayList<>();
        for (ProtoField field : protoField.getFields()) {
            BQField bqField = new BQField(field);
            if (field.isNested()) {
                List<Field> fields = generateBigquerySchema(field);
                bqField.setSubFields(fields);
            }
            schemaFields.add(bqField.getField());
        }
        return schemaFields;
    }
}
