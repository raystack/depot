package io.odpf.depot.bigquery.table;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import io.odpf.depot.bigquery.exception.BQClusteringKeysException;
import io.odpf.depot.config.BigQuerySinkConfig;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryTableClustering {

    private static final int MAX_CLUSTERING_KEYS = 4;
    private final BigQuerySinkConfig bqConfig;


    public BigQueryTableClustering(BigQuerySinkConfig bqConfig) {
        this.bqConfig = bqConfig;
    }

    public Clustering getClusteredTableDefinition(Schema schema) {
        if (bqConfig.getTableClusteringKey() == null) {
            throw new BQClusteringKeysException(String.format("Clustering key not specified for the table: %s", bqConfig.getTableName()));
        }
        List<String> columnNames = Arrays.asList(bqConfig.getTableClusteringKey()
                .replaceAll("\\s+", "")
                .split(","));
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
