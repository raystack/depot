package io.odpf.depot.bigtable.client;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.config.BigTableSinkConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class BigTableClient {
    private final BigtableTableAdminClient bigtableTableAdminClient;
    private final BigtableDataClient bigtableDataClient;
    private final BigTableSinkConfig sinkConfig;

    public BigTableClient(BigTableSinkConfig sinkConfig) throws IOException {
        this.sinkConfig = sinkConfig;
        this.bigtableTableAdminClient = getBigTableAdminClient(sinkConfig);
        this.bigtableDataClient = getBigTableDataClient(sinkConfig);
    }

    private BigtableDataClient getBigTableDataClient(BigTableSinkConfig sinkConfig) throws IOException {
        BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                .setProjectId(sinkConfig.getGCloudProjectID())
                .setInstanceId(sinkConfig.getBigtableInstanceId())
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getBigTableCredentialPath()))))
                .build();
        return BigtableDataClient.create(settings);
    }

    private BigtableTableAdminClient getBigTableAdminClient(BigTableSinkConfig sinkConfig) throws IOException {
        BigtableTableAdminSettings settings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(sinkConfig.getGCloudProjectID())
                .setInstanceId(sinkConfig.getBigtableInstanceId())
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getBigTableCredentialPath()))))
                .build();

        return BigtableTableAdminClient.create(settings);
    }

    public BigTableResponse send(List<BigTableRecord> records) throws MutateRowsException {
        BulkMutation batch = BulkMutation.create(sinkConfig.getTableId());
        BigTableResponse bigTableResponse = null;
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

    public boolean tableExists(String tableId) {
        return bigtableTableAdminClient.exists(tableId);
    }

}
