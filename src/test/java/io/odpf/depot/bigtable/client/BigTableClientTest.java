package io.odpf.depot.bigtable.client;

import com.google.api.gax.rpc.ApiException;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.odpf.depot.bigtable.exception.BigTableInvalidSchemaException;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.model.BigTableSchema;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

public class BigTableClientTest {

    @Mock
    private BigtableDataClient bigTableDataClient;
    @Mock
    private BigtableTableAdminClient bigtableTableAdminClient;
    @Mock
    private ApiException apiException;

    private BigTableClient bigTableClient;
    private List<BigTableRecord> validRecords;
    private BigTableSinkConfig sinkConfig;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        System.setProperty("SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID", "test-gcloud-project");
        System.setProperty("SINK_BIGTABLE_INSTANCE_ID", "test-instance");
        System.setProperty("SINK_BIGTABLE_TABLE_ID", "test-table");
        System.setProperty("SINK_BIGTABLE_CREDENTIAL_PATH", "Users/github/bigtable/test-credential");
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"family-test\" : { \"qualifier_name1\" : \"input_field1\", \"qualifier_name2\" : \"input_field2\"} }");
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key-constant-string");

        RowMutationEntry rowMutationEntry = RowMutationEntry.create("rowKey").setCell("family", "qualifier", "value");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry, 1, null, true);
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry, 2, null, true);
        validRecords = Collections.list(bigTableRecord1, bigTableRecord2);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        BigTableSchema schema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableClient = new BigTableClient(sinkConfig, bigTableDataClient, bigtableTableAdminClient, schema);
    }

    @Test
    public void shouldReturnNullBigTableResponseWhenBulkMutateRowsDoesNotThrowAnException() {
        doNothing().when(bigTableDataClient).bulkMutateRows(isA(BulkMutation.class));

        BigTableResponse bigTableResponse = bigTableClient.send(validRecords);

        Assert.assertNull(bigTableResponse);
    }

    @Test
    public void shouldReturnBigTableResponseWithFailedMutationsWhenBulkMutateRowsThrowsMutateRowsException() {
        List<MutateRowsException.FailedMutation> failedMutations = new ArrayList<>();
        failedMutations.add(MutateRowsException.FailedMutation.create(0, apiException));
        failedMutations.add(MutateRowsException.FailedMutation.create(1, apiException));
        MutateRowsException mutateRowsException = new MutateRowsException(null, failedMutations, false);

        doThrow(mutateRowsException).when(bigTableDataClient).bulkMutateRows(isA(BulkMutation.class));

        BigTableResponse bigTableResponse = bigTableClient.send(validRecords);

        Assert.assertTrue(bigTableResponse.hasErrors());
        Assert.assertEquals(2, bigTableResponse.getFailedMutations().size());
    }

    @Test
    public void shouldThrowInvalidSchemaExceptionIfTableDoesNotExist() {
        when(bigtableTableAdminClient.exists(sinkConfig.getTableId())).thenReturn(false);
        try {
            bigTableClient.validateBigTableSchema();
        } catch (BigTableInvalidSchemaException e) {
            Assert.assertEquals("Table: " + sinkConfig.getTableId() + " does not exist", e.getMessage());
        }
    }

    @Test
    public void shouldThrowInvalidSchemaExceptionIfColumnFamilyDoesNotExist() {
        Table testTable = Table.fromProto(com.google.bigtable.admin.v2.Table.newBuilder()
                .setName("projects/" + sinkConfig.getGCloudProjectID() + "/instances/" + sinkConfig.getInstanceId() + "/tables/" + sinkConfig.getTableId())
                .putColumnFamilies("existing-family-test", ColumnFamily.newBuilder().build())
                .build());

        when(bigtableTableAdminClient.exists(sinkConfig.getTableId())).thenReturn(true);
        when(bigtableTableAdminClient.getTable(sinkConfig.getTableId())).thenReturn(testTable);
        try {
            bigTableClient.validateBigTableSchema();
        } catch (BigTableInvalidSchemaException e) {
            Assert.assertEquals("Column families [family-test] do not exist in table test-table!", e.getMessage());
        }
    }
}
