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
import io.odpf.depot.bigtable.model.BigtableSchema;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.config.BigTableSinkConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BigTableClient {
    private final BigtableTableAdminClient bigtableTableAdminClient;
    private final BigtableDataClient bigtableDataClient;
    private final BigTableSinkConfig sinkConfig;
    private final BigtableSchema bigtableSchema;

    public BigTableClient(BigTableSinkConfig sinkConfig, BigtableSchema bigtableSchema) throws IOException {
        this(sinkConfig, getBigTableDataClient(sinkConfig), getBigTableAdminClient(sinkConfig), bigtableSchema);
    }

    public BigTableClient(BigTableSinkConfig sinkConfig, BigtableDataClient bigtableDataClient, BigtableTableAdminClient bigtableTableAdminClient, BigtableSchema bigtableSchema) {
        this.sinkConfig = sinkConfig;
        this.bigtableDataClient = bigtableDataClient;
        this.bigtableTableAdminClient = bigtableTableAdminClient;
        this.bigtableSchema = bigtableSchema;
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

    public BigTableResponse send(List<BigTableRecord> records) throws MutateRowsException {
        BigTableResponse bigTableResponse = null;
        BulkMutation batch = BulkMutation.create(sinkConfig.getTableId());
        for (BigTableRecord record : records) {
            batch.add(record.getRowMutationEntry());
        }
        try {
            bigtableDataClient.bulkMutateRows(batch);
        } catch (MutateRowsException e) {
            List<MutateRowsException.FailedMutation> failedMutations = e.getFailedMutations();
            bigTableResponse = new BigTableResponse(failedMutations);
        }

        return bigTableResponse;
    }

    public void validateBigTableSchema() throws BigTableInvalidSchemaException {
        String tableId = sinkConfig.getTableId();
        checkIfTableExists(tableId);
        checkIfColumnFamiliesExist(tableId);
    }

    private void checkIfTableExists(String tableId) throws BigTableInvalidSchemaException {
        if (!bigtableTableAdminClient.exists(tableId)) {
            throw new BigTableInvalidSchemaException(String.format("Table: %s does not exist", tableId));
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
