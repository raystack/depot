package io.odpf.depot.bigquery.table;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import io.odpf.depot.bigquery.exception.BQPartitionKeyNotSpecified;
import io.odpf.depot.config.BigQuerySinkConfig;

import java.util.Optional;

public class BigQueryTablePartitioning {

    private final BigQuerySinkConfig bqConfig;

    public BigQueryTablePartitioning(BigQuerySinkConfig bqConfig) {
        this.bqConfig = bqConfig;
    }

    public TimePartitioning getPartitionedTableDefinition(Schema schema) {
        Optional<Field> partitionFieldOptional = schema.getFields().stream().filter(obj -> obj.getName().equals(bqConfig.getTablePartitionKey())).findFirst();
        if (!partitionFieldOptional.isPresent()) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key %s is not present in the schema", bqConfig.getTablePartitionKey()));
        }

        Field partitionField = partitionFieldOptional.get();
        if (isTimePartitionedField(partitionField)) {
            return createTimePartitionBuilder().build();
        } else {
            throw new UnsupportedOperationException("Range BigQuery partitioning is not supported, supported partition fields have to be of DATE or TIMESTAMP type");
        }
    }

    private TimePartitioning.Builder createTimePartitionBuilder() {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        if (bqConfig.getTablePartitionKey() == null) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key not specified for the table: %s", bqConfig.getTableName()));
        }
        timePartitioningBuilder.setField(bqConfig.getTablePartitionKey())
                .setRequirePartitionFilter(true);

        Long neverExpireMillis = null;
        Long partitionExpiry = bqConfig.getBigQueryTablePartitionExpiryMS() > 0 ? bqConfig.getBigQueryTablePartitionExpiryMS() : neverExpireMillis;
        timePartitioningBuilder.setExpirationMs(partitionExpiry);

        return timePartitioningBuilder;
    }

    private boolean isTimePartitionedField(Field partitionField) {
        return partitionField.getType() == LegacySQLTypeName.TIMESTAMP || partitionField.getType() == LegacySQLTypeName.DATE;
    }
}
