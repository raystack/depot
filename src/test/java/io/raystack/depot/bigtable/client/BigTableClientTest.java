package org.raystack.depot.bigtable.client;

import com.google.api.gax.rpc.ApiException;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import org.raystack.depot.TestBookingLogKey;
import org.raystack.depot.TestBookingLogMessage;
import org.raystack.depot.TestServiceType;
import org.raystack.depot.bigtable.exception.BigTableInvalidSchemaException;
import org.raystack.depot.bigtable.model.BigTableRecord;
import org.raystack.depot.bigtable.model.BigTableSchema;
import org.raystack.depot.bigtable.response.BigTableResponse;
import org.raystack.depot.config.BigTableSinkConfig;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.metrics.BigTableMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

public class BigTableClientTest {

        @Mock
        private BigtableDataClient bigTableDataClient;
        @Mock
        private BigtableTableAdminClient bigtableTableAdminClient;
        @Mock
        private ApiException apiException;
        @Mock
        private BigTableMetrics bigtableMetrics;
        @Mock
        private Instrumentation instrumentation;

        private BigTableClient bigTableClient;
        private List<BigTableRecord> validRecords;
        private BigTableSinkConfig sinkConfig;

        @Before
        public void setUp() throws IOException {
                MockitoAnnotations.openMocks(this);
                System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS",
                                "org.raystack.depot.TestBookingLogMessage");
                System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE",
                                String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
                System.setProperty("SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID", "test-gcloud-project");
                System.setProperty("SINK_BIGTABLE_INSTANCE_ID", "test-instance");
                System.setProperty("SINK_BIGTABLE_TABLE_ID", "test-table");
                System.setProperty("SINK_BIGTABLE_CREDENTIAL_PATH", "Users/github/bigtable/test-credential");
                System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING",
                                "{ \"family-test\" : { \"qualifier_name1\" : \"input_field1\", \"qualifier_name2\" : \"input_field2\"} }");
                System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key-constant-string");

                TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1")
                                .setOrderUrl("order-url#1").build();
                TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1")
                                .setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
                TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2")
                                .setOrderUrl("order-url#2").build();
                TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2")
                                .setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

                RaystackMessage message1 = new RaystackMessage(bookingLogKey1.toByteArray(),
                                bookingLogMessage1.toByteArray());
                RaystackMessage message2 = new RaystackMessage(bookingLogKey2.toByteArray(),
                                bookingLogMessage2.toByteArray());

                RowMutationEntry rowMutationEntry = RowMutationEntry.create("rowKey").setCell("family", "qualifier",
                                "value");
                BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry, 1, null, message1.getMetadata());
                BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry, 2, null, message2.getMetadata());
                validRecords = Collections.list(bigTableRecord1, bigTableRecord2);
                sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
                BigTableSchema schema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
                bigTableClient = new BigTableClient(sinkConfig, bigTableDataClient, bigtableTableAdminClient, schema,
                                bigtableMetrics, instrumentation);
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
                Mockito.verify(instrumentation, Mockito.times(1)).logError("Some entries failed to be applied. {}",
                                mutateRowsException.getCause());
        }

    @Test
    public void shouldThrowInvalidSchemaExceptionIfTableDoesNotExist() {
        when(bigtableTableAdminClient.exists(sinkConfig.getTableId())).thenReturn(false);
        when(bigtableTableAdminClient.getProjectId()).thenReturn(sinkConfig.getGCloudProjectID());
        when(bigtableTableAdminClient.getInstanceId()).thenReturn(sinkConfig.getInstanceId());
        try {
            bigTableClient.validateBigTableSchema();
        } catch (BigTableInvalidSchemaException e) {
            Assert.assertEquals("Table not found on the path: projects/test-gcloud-project/instances/test-instance/tables/test-table", e.getMessage());
        }
    }

        @Test
        public void shouldThrowInvalidSchemaExceptionIfColumnFamilyDoesNotExist() {
                Table testTable = Table.fromProto(com.google.bigtable.admin.v2.Table.newBuilder()
                                .setName("projects/" + sinkConfig.getGCloudProjectID() + "/instances/"
                                                + sinkConfig.getInstanceId()
                                                + "/tables/" + sinkConfig.getTableId())
                                .putColumnFamilies("existing-family-test", ColumnFamily.newBuilder().build())
                                .build());

                when(bigtableTableAdminClient.exists(sinkConfig.getTableId())).thenReturn(true);
                when(bigtableTableAdminClient.getTable(sinkConfig.getTableId())).thenReturn(testTable);
                try {
                        bigTableClient.validateBigTableSchema();
                } catch (BigTableInvalidSchemaException e) {
                        Assert.assertEquals("Column families [family-test] do not exist in table test-table!",
                                        e.getMessage());
                }
        }

        @Test
        public void shouldCaptureBigtableMetricsWhenBulkMutateRowsDoesNotThrowAnException() {
                doNothing().when(bigTableDataClient).bulkMutateRows(isA(BulkMutation.class));

                bigTableClient.send(validRecords);

                Mockito.verify(instrumentation, Mockito.times(1)).captureDurationSince(
                                eq(bigtableMetrics.getBigtableOperationLatencyMetric()),
                                any(),
                                eq(String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId())),
                                eq(String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId())));
                Mockito.verify(instrumentation, Mockito.times(1)).captureCount(
                                eq(bigtableMetrics.getBigtableOperationTotalMetric()),
                                any(),
                                eq(String.format(BigTableMetrics.BIGTABLE_INSTANCE_TAG, sinkConfig.getInstanceId())),
                                eq(String.format(BigTableMetrics.BIGTABLE_TABLE_TAG, sinkConfig.getTableId())));
        }
}
