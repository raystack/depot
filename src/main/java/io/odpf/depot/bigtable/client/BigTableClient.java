package io.odpf.depot.bigtable.client;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import io.odpf.depot.bigtable.exception.BigTableInvalidSchemaException;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.model.BigTableSchema;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.metrics.BigTableMetrics;
import io.odpf.depot.metrics.Instrumentation;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BigTableClient {
    private final BigtableTableAdminClient bigtableTableAdminClient;
    private final BigtableDataClient bigtableDataClient;
    private final BigTableSinkConfig sinkConfig;
    private final BigTableSchema bigtableSchema;
    private final BigTableMetrics bigtableMetrics;
    private final Instrumentation instrumentation;

    public BigTableClient(BigTableSinkConfig sinkConfig, BigTableSchema bigtableSchema, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) throws IOException {
        this(sinkConfig, getBigTableDataClient(sinkConfig), getBigTableAdminClient(sinkConfig), bigtableSchema, bigtableMetrics, instrumentation);
    }

    public BigTableClient(BigTableSinkConfig sinkConfig, BigtableDataClient bigtableDataClient, BigtableTableAdminClient bigtableTableAdminClient, BigTableSchema bigtableSchema, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        this.sinkConfig = sinkConfig;
        this.bigtableDataClient = bigtableDataClient;
        this.bigtableTableAdminClient = bigtableTableAdminClient;
        this.bigtableSchema = bigtableSchema;
        this.bigtableMetrics = bigtableMetrics;
        this.instrumentation = instrumentation;
    }

    private static BigtableDataClient getBigTableDataClient(BigTableSinkConfig sinkConfig) throws IOException {
        BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                .setProjectId(sinkConfig.getGCloudProjectID())
                .setInstanceId(sinkConfig.getInstanceId())
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getCredentialPath()))))
                .build();
        return BigtableDataClient.create(settings);
    }

    private static BigtableTableAdminClient getBigTableAdminClient(BigTableSinkConfig sinkConfig) throws IOException {
        BigtableTableAdminSettings settings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(sinkConfig.getGCloudProjectID())
                .setInstanceId(sinkConfig.getInstanceId())
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getCredentialPath()))))
                .build();
        return BigtableTableAdminClient.create(settings);
    }

    public BigTableResponse send(List<BigTableRecord> records) {
        BigTableResponse bigTableResponse = null;
        BulkMutation batch = BulkMutation.create(sinkConfig.getTableId());
        for (BigTableRecord record : records) {
            batch.add(record.getRowMutationEntry());
        }
        try {
            Instant startTime = Instant.now();
            bigtableDataClient.bulkMutateRows(batch);

            instrumentation.captureDurationSince(
                    bigtableMetrics.getBigtableOperationLatencyMetric(),
                    startTime,
                    String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId()),
                    String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId()));
            instrumentation.captureCount(
                    bigtableMetrics.getBigtableOperationTotalMetric(),
                    (long) batch.getEntryCount(),
                    String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId()),
                    String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId()));
        } catch (MutateRowsException e) {
            bigTableResponse = new BigTableResponse(e);
            instrumentation.logError("Some entries failed to be applied. {}", e.getCause());
        }

        return bigTableResponse;
    }

    public void validateBigTableSchema() throws BigTableInvalidSchemaException {
        String tableId = sinkConfig.getTableId();
        instrumentation.logDebug(String.format("Validating schema for table: %s...", tableId));
        checkIfTableExists(tableId);
        checkIfColumnFamiliesExist(tableId);
        instrumentation.logDebug("Validation complete, Schema is valid.");
    }

    private void checkIfTableExists(String tableId) throws BigTableInvalidSchemaException {
        if (!bigtableTableAdminClient.exists(tableId)) {
            throw new BigTableInvalidSchemaException(String.format("Table: %s does not exist!", tableId));
        }
    }

    private void checkIfColumnFamiliesExist(String tableId) throws BigTableInvalidSchemaException {
        Set<String> existingColumnFamilies = bigtableTableAdminClient.getTable(tableId)
                .getColumnFamilies()
                .stream()
                .map(ColumnFamily::getId)
                .collect(Collectors.toSet());
        Set<String> missingColumnFamilies = bigtableSchema.getMissingColumnFamilies(existingColumnFamilies);
        if (missingColumnFamilies.size() > 0) {
            throw new BigTableInvalidSchemaException(
                    String.format("Column families %s do not exist in table %s!", missingColumnFamilies, tableId));
        }
    }
}
