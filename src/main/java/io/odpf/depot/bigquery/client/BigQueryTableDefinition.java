package io.odpf.depot.bigquery.client;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.depot.bigquery.exception.BQClusteringKeysException;
import io.odpf.depot.bigquery.exception.BQPartitionKeyNotSpecified;
import io.odpf.depot.config.BigQuerySinkConfig;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public class BigQueryTableDefinition {

    private static final int MAX_CLUSTERING_KEYS = 4;
    private final BigQuerySinkConfig bqConfig;

    public StandardTableDefinition getTableDefinition(Schema schema) {
        StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition.newBuilder()
                .setSchema(schema);

        if (bqConfig.isTablePartitioningEnabled()) {
            TimePartitioning partitioning = getPartitionedTableDefinition(schema);
            tableDefinitionBuilder.setTimePartitioning(partitioning);
        }
        if (bqConfig.isTableClusteringEnabled()) {
            Clustering clustering = getClusteredTableDefinition(schema);
            tableDefinitionBuilder.setClustering(clustering);
        }
        return tableDefinitionBuilder.build();
    }

    private TimePartitioning getPartitionedTableDefinition(Schema schema) {
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

    private Clustering getClusteredTableDefinition(Schema schema) {
        if (bqConfig.getTableClusteringKeys().isEmpty()) {
            throw new BQClusteringKeysException(String.format("Clustering key not specified for the table: %s", bqConfig.getTableName()));
        }
        List<String> columnNames = bqConfig.getTableClusteringKeys();
        if (columnNames.size() > MAX_CLUSTERING_KEYS) {
            throw new BQClusteringKeysException(String.format("Max number of columns for clustering is %d", MAX_CLUSTERING_KEYS));
        }
        FieldList fields = schema.getFields();
        List<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toList());
        if (!fieldNames.containsAll(columnNames)) {
            throw new BQClusteringKeysException(String.format("One or more column names specified %s not exist on the schema or a nested type which is not supported for clustering", columnNames));
        }

        return Clustering.newBuilder()
                .setFields(columnNames)
                .build();
    }
}
