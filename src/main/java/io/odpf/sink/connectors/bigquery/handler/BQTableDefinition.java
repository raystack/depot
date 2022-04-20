package io.odpf.sink.connectors.bigquery.handler;

import com.google.cloud.bigquery.*;
import io.odpf.sink.connectors.bigquery.exception.BQPartitionKeyNotSpecified;
import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class BQTableDefinition {
    private final BigQuerySinkConfig bqConfig;

    public StandardTableDefinition getTableDefinition(Schema schema) {
        StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
        if (!bqConfig.isTablePartitioningEnabled()) {
            return tableDefinition;
        }
        return getPartitionedTableDefinition(schema);
    }

    private StandardTableDefinition getPartitionedTableDefinition(Schema schema) {
        StandardTableDefinition.Builder tableDefinition = StandardTableDefinition.newBuilder();
        Optional<Field> partitionFieldOptional = schema.getFields().stream().filter(obj -> obj.getName().equals(bqConfig.getTablePartitionKey())).findFirst();
        if (!partitionFieldOptional.isPresent()) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key %s is not present in the schema", bqConfig.getTablePartitionKey()));
        }

        Field partitionField = partitionFieldOptional.get();
        if (isTimePartitionedField(partitionField)) {
            return createTimePartitionBuilder(tableDefinition)
                    .setSchema(schema)
                    .build();
        } else {
            throw new UnsupportedOperationException("Range Bigquery partitioning is not supported, supported paritition fields have to be of DATE or TIMESTAMP type");
        }
    }

    private StandardTableDefinition.Builder createTimePartitionBuilder(StandardTableDefinition.Builder tableBuilder) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        if (bqConfig.getTablePartitionKey() == null) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key not specified for the table: %s", bqConfig.getTableName()));
        }
        timePartitioningBuilder.setField(bqConfig.getTablePartitionKey())
                .setRequirePartitionFilter(true);

        Long neverExpireMillis = null;
        Long partitionExpiry = bqConfig.getBigQueryTablePartitionExpiryMS() > 0 ? bqConfig.getBigQueryTablePartitionExpiryMS() : neverExpireMillis;
        timePartitioningBuilder.setExpirationMs(partitionExpiry);

        return tableBuilder
                .setTimePartitioning(timePartitioningBuilder.build());
    }

    private boolean isTimePartitionedField(Field partitionField) {
        return partitionField.getType() == LegacySQLTypeName.TIMESTAMP || partitionField.getType() == LegacySQLTypeName.DATE;
    }
}
