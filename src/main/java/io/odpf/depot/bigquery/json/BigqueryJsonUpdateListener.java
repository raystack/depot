package io.odpf.depot.bigquery.json;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import io.odpf.depot.bigquery.handler.BigQueryClient;
import io.odpf.depot.bigquery.handler.MessageRecordConverter;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import io.odpf.depot.common.TupleString;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BigqueryJsonUpdateListener extends OdpfStencilUpdateListener {
    private final MessageRecordConverterCache converterCache;
    private BigQuerySinkConfig config;
    private final BigQueryClient bigQueryClient;

    public BigqueryJsonUpdateListener(BigQuerySinkConfig config, MessageRecordConverterCache converterCache, BigQueryClient bigQueryClient) {
        this.converterCache = converterCache;
        this.config = config;
        this.bigQueryClient = bigQueryClient;
        if (!config.getSinkConnectorSchemaJsonDynamicSchemaEnable()) {
            throw new UnsupportedOperationException("currently only schema inferred from incoming data is supported, stencil schema support for json will be added in future");
        }
    }

    @Override
    public void updateSchema() {
        OdpfMessageParser parser = getOdpfMessageParser();
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(parser, config, null);
        converterCache.setMessageRecordConverter(messageRecordConverter);
        Schema existingTableSchema = bigQueryClient.getSchema();
        FieldList existingTableFields = existingTableSchema.getFields();
        List<TupleString> defaultColumns = config.getSinkBigquerySchemaJsonOutputDefaultColumns();
        Set<Field> defaultFields = defaultColumns
                .stream()
                .map(tupleString -> getField(tupleString))
                .collect(Collectors.toSet());
        Set<Field> finalFieldList = new HashSet<Field>(defaultFields);
        existingTableFields.iterator().forEachRemaining(finalFieldList::add);
        bigQueryClient.upsertTable(new ArrayList<>(finalFieldList));
    }

    private Field getField(TupleString tupleString) {
        String fieldName = tupleString.getFirst();
        LegacySQLTypeName fieldDataType = LegacySQLTypeName.valueOfStrict(tupleString.getSecond().toUpperCase());

        if (isValidPartitionField(fieldName, fieldDataType)) {
            return Field.of(fieldName, fieldDataType);
        }

        if (config.getSinkConnectorSchemaJsonOutputDefaultDatatypeStringEnable()
                && fieldDataType != LegacySQLTypeName.STRING) {
            throw new IllegalArgumentException("default columns data type should be string "
                    + "when config for json output default data type string is enabled");
        }

        return Field.of(fieldName, fieldDataType);
    }

    private boolean isValidPartitionField(String fieldName, LegacySQLTypeName fieldDataType) {
        Boolean isPartitioningEnabled = config.isTablePartitioningEnabled();
        String partitionKey = config.getTablePartitionKey();
        /*
        BQTableDefinition.getPartitionedTableDefinition()
        throws UnsupportedOperationException("Range Bigquery partitioning is not supported, supported paritition fields have to be of DATE or TIMESTAMP type");
         */
        boolean isValidPartitionDataType = (fieldDataType == LegacySQLTypeName.TIMESTAMP || fieldDataType == LegacySQLTypeName.DATE);

        return isPartitioningEnabled && partitionKey.equals(fieldName) && isValidPartitionDataType;
    }
}
