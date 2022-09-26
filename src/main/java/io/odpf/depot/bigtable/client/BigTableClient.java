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
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.config.BigTableSinkConfig;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BigTableClient {
    private final BigtableTableAdminClient bigtableTableAdminClient;
    private final BigtableDataClient bigtableDataClient;
    private final BigTableSinkConfig sinkConfig;

    public BigTableClient(BigTableSinkConfig sinkConfig) throws IOException {
        this(sinkConfig, getBigTableDataClient(sinkConfig), getBigTableAdminClient(sinkConfig));
    }

    public BigTableClient(BigTableSinkConfig sinkConfig, BigtableDataClient bigtableDataClient, BigtableTableAdminClient bigtableTableAdminClient) {
        this.sinkConfig = sinkConfig;
        this.bigtableDataClient = bigtableDataClient;
        this.bigtableTableAdminClient = bigtableTableAdminClient;
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
        this.tableExists(sinkConfig.getTableId());
        this.columnFamiliesExist(sinkConfig.getColumnFamilyMapping(), sinkConfig.getTableId());
    }

    private void tableExists(String tableId) throws BigTableInvalidSchemaException {
        if (!bigtableTableAdminClient.exists(tableId)) {
            throw new BigTableInvalidSchemaException(String.format("Table: %s does not exist", tableId));
        }
    }

    private void columnFamiliesExist(String inputOutputFieldMapping, String tableId) throws BigTableInvalidSchemaException {
        List<String> existingColumnFamilies = bigtableTableAdminClient.getTable(tableId)
                .getColumnFamilies()
                .stream()
                .map(ColumnFamily::getId)
                .collect(Collectors.toList());

        Set<String> columnFamilies = new JSONObject(inputOutputFieldMapping).keySet();
        for (String columnFamily : columnFamilies) {
            if (!existingColumnFamilies.contains(columnFamily)) {
                throw new BigTableInvalidSchemaException(String.format("Column family: %s does not exist!", columnFamily));
            }
        }
    }
}
