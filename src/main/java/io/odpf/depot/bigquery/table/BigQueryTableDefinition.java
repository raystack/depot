package io.odpf.depot.bigquery.table;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.depot.config.BigQuerySinkConfig;

public class BigQueryTableDefinition {
    private final BigQuerySinkConfig bqConfig;
    private final BigQueryTablePartitioning tablePartitioning;
    private final BigQueryTableClustering tableClustering;

    public BigQueryTableDefinition(BigQuerySinkConfig bqConfig) {
        this.bqConfig = bqConfig;
        tablePartitioning = new BigQueryTablePartitioning(bqConfig);
        tableClustering = new BigQueryTableClustering(bqConfig);
    }

    public StandardTableDefinition getTableDefinition(Schema schema) {
        StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition.newBuilder()
                .setSchema(schema);
        if (bqConfig.isTablePartitioningEnabled()) {
            TimePartitioning partitioning = tablePartitioning.getPartitionedTableDefinition(schema);
            tableDefinitionBuilder.setTimePartitioning(partitioning);
        }
        if (bqConfig.isTableClusteringEnabled()) {
            Clustering clustering = tableClustering.getClusteredTableDefinition(schema);
            tableDefinitionBuilder.setClustering(clustering);
        }

        return tableDefinitionBuilder.build();
    }
}
