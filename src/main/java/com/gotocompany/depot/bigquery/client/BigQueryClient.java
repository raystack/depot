package com.gotocompany.depot.bigquery.client;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.gotocompany.depot.bigquery.exception.BQDatasetLocationChangedException;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import lombok.Getter;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Random;

public class BigQueryClient {
    private static final int TABLE_INFO_UPDATE_RETRIES = 10;
    private static final int DEFAULT_SLEEP_RETRY = 10000;
    private final BigQuery bigquery;
    @Getter
    private final TableId tableID;
    private final BigQuerySinkConfig bqConfig;
    private final BigQueryTableDefinition bigQueryTableDefinition;
    private final Instrumentation instrumentation;
    private final Random random = new Random(System.currentTimeMillis());
    private final BigQueryMetrics bigqueryMetrics;

    public BigQueryClient(BigQuerySinkConfig bqConfig, BigQueryMetrics bigQueryMetrics, Instrumentation instrumentation) throws IOException {
        this(getBigQueryInstance(bqConfig), bqConfig, bigQueryMetrics, instrumentation);
    }

    public BigQueryClient(BigQuery bq, BigQuerySinkConfig bqConfig, BigQueryMetrics bigQueryMetrics, Instrumentation instrumentation) {
        this.bigquery = bq;
        this.bqConfig = bqConfig;
        this.tableID = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        this.bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        this.instrumentation = instrumentation;
        this.bigqueryMetrics = bigQueryMetrics;
    }

    private static BigQuery getBigQueryInstance(BigQuerySinkConfig sinkConfig) throws IOException {
        TransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions().toBuilder()
                .setConnectTimeout(sinkConfig.getBqClientConnectTimeoutMS())
                .setReadTimeout(sinkConfig.getBqClientReadTimeoutMS())
                .build();
        return BigQueryOptions.newBuilder()
                .setTransportOptions(transportOptions)
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getBigQueryCredentialPath())))
                .setProjectId(sinkConfig.getGCloudProjectID())
                .build().getService();
    }

    public InsertAllResponse insertAll(InsertAllRequest rows) {
        Instant start = Instant.now();
        InsertAllResponse response = bigquery.insertAll(rows);
        instrument(start, BigQueryMetrics.BigQueryAPIType.TABLE_INSERT_ALL);
        return response;
    }

    public void upsertTable(List<Field> bqSchemaFields) throws BigQueryException {
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition)
                .setLabels(bqConfig.getTableLabels())
                .build();
        upsertDatasetAndTableWithRetry(tableInfo);
    }

    public Schema getSchema() {
        Table table = bigquery.getTable(tableID);
        if (table == null) {
            return Schema.of();
        }
        return table.getDefinition().getSchema();
    }

    private void upsertDatasetAndTableWithRetry(TableInfo info) {
        for (int ii = 0; ii < TABLE_INFO_UPDATE_RETRIES; ii++) {
            try {
                upsertDatasetAndTable(info);
                return;
            } catch (BigQueryException e) {
                instrumentation.logWarn(e.getMessage());
                if (e.getMessage().contains("Exceeded rate limits")) {
                    try {
                        int sleepMillis = random.nextInt(DEFAULT_SLEEP_RETRY);
                        instrumentation.logInfo("Waiting for " + sleepMillis + " milliseconds");
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException interruptedException) {
                        instrumentation.captureNonFatalError(bigqueryMetrics.getErrorEventMetric(), interruptedException, "Sleep interrupted");
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    private void upsertDatasetAndTable(TableInfo tableInfo) {
        Dataset dataSet = bigquery.getDataset(tableID.getDataset());
        if (dataSet == null || !bigquery.getDataset(tableID.getDataset()).exists()) {
            Instant start = Instant.now();
            bigquery.create(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLocation(bqConfig.getBigQueryDatasetLocation())
                            .setLabels(bqConfig.getDatasetLabels())
                            .build()
            );
            instrumentation.logInfo("Successfully CREATED bigquery DATASET: {}", tableID.getDataset());
            instrument(start, BigQueryMetrics.BigQueryAPIType.DATASET_CREATE);
        } else if (shouldUpdateDataset(dataSet)) {
            Instant start = Instant.now();
            bigquery.update(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLabels(bqConfig.getDatasetLabels())
                            .build()
            );
            instrumentation.logInfo("Successfully UPDATED bigquery DATASET: {} with labels", tableID.getDataset());
            instrument(start, BigQueryMetrics.BigQueryAPIType.DATASET_UPDATE);
        }

        Table table = bigquery.getTable(tableID);
        if (table == null || !table.exists()) {
            Instant start = Instant.now();
            bigquery.create(tableInfo);
            instrumentation.logInfo("Successfully CREATED bigquery TABLE: {}", tableID.getTable());
            instrument(start, BigQueryMetrics.BigQueryAPIType.TABLE_CREATE);
        } else {
            Schema existingSchema = table.getDefinition().getSchema();
            Schema updatedSchema = tableInfo.getDefinition().getSchema();

            if (shouldUpdateTable(tableInfo, table, existingSchema, updatedSchema)) {
                Instant start = Instant.now();
                bigquery.update(tableInfo);
                instrumentation.logInfo("Successfully UPDATED bigquery TABLE: {}", tableID.getTable());
                instrument(start, BigQueryMetrics.BigQueryAPIType.TABLE_UPDATE);
            } else {
                instrumentation.logInfo("Skipping bigquery table update, since proto schema hasn't changed");
            }
        }
    }

    private void instrument(Instant startTime, BigQueryMetrics.BigQueryAPIType type) {
        instrumentation.incrementCounter(
                bigqueryMetrics.getBigqueryOperationTotalMetric(),
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, tableID.getTable()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, tableID.getDataset()),
                String.format(BigQueryMetrics.BIGQUERY_API_TAG, type));
        instrumentation.captureDurationSince(
                bigqueryMetrics.getBigqueryOperationLatencyMetric(),
                startTime,
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, tableID.getTable()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, tableID.getDataset()),
                String.format(BigQueryMetrics.BIGQUERY_API_TAG, type));
    }

    private boolean shouldUpdateTable(TableInfo tableInfo, Table table, Schema existingSchema, Schema updatedSchema) {
        return !table.getLabels().equals(tableInfo.getLabels())
                || !existingSchema.equals(updatedSchema)
                || shouldChangePartitionExpiryForStandardTable(table)
                || shouldUpdateClusteringKeys(table);
    }

    private boolean shouldUpdateDataset(Dataset dataSet) {
        if (!dataSet.getLocation().equals(bqConfig.getBigQueryDatasetLocation())) {
            throw new BQDatasetLocationChangedException("Dataset location cannot be changed from "
                    + dataSet.getLocation() + " to " + bqConfig.getBigQueryDatasetLocation());
        }

        return !dataSet.getLabels().equals(bqConfig.getDatasetLabels());
    }

    private boolean shouldChangePartitionExpiryForStandardTable(Table table) {
        if (!isTable(table)) {
            return false;
        }
        TimePartitioning timePartitioning = ((StandardTableDefinition) (table.getDefinition())).getTimePartitioning();
        if (timePartitioning == null) {
            // If the table is not partitioned already, no need to update the table
            return false;
        }
        long neverExpireMs = 0L;
        Long currentExpirationMs = timePartitioning.getExpirationMs() == null ? neverExpireMs : timePartitioning.getExpirationMs();
        Long newExpirationMs = bqConfig.getBigQueryTablePartitionExpiryMS() > 0 ? bqConfig.getBigQueryTablePartitionExpiryMS() : neverExpireMs;
        return !currentExpirationMs.equals(newExpirationMs);
    }

    private boolean shouldUpdateClusteringKeys(Table table) {
        if (!isTable(table)) {
            return false;
        }
        Clustering clustering = ((StandardTableDefinition) (table.getDefinition())).getClustering();
        if (clustering != null) {
            List<String> existingClusteringKeys = clustering.getFields();
            if (bqConfig.isTableClusteringEnabled()) {
                List<String> updatedClusteringKeys = bqConfig.getTableClusteringKeys();
                return !existingClusteringKeys.equals(updatedClusteringKeys);
            } else {
                return false;
            }
        } else {
            return bqConfig.isTableClusteringEnabled();
        }
    }

    private boolean isTable(Table table) {
        return table.getDefinition().getType().equals(StandardTableDefinition.Type.TABLE);
    }

    private TableDefinition getTableDefinition(Schema schema) {
        return bigQueryTableDefinition.getTableDefinition(schema);
    }
}
