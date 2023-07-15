package org.raystack.depot.bigquery.client;

import com.google.cloud.bigquery.*;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.metrics.BigQueryMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryClientTest {

    @Mock
    private BigQuery bigquery;
    @Mock
    private BigQuerySinkConfig bqConfig;
    @Mock
    private Dataset dataset;
    @Mock
    private Table table;
    @Mock
    private StandardTableDefinition mockTableDefinition;
    @Mock
    private TimePartitioning mockTimePartitioning;
    @Mock
    private Clustering mockClustering;
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private BigQueryMetrics metrics;

    private BigQueryClient bqClient;

    @Test
    public void shouldIgnoreExceptionIfDatasetAlreadyExists() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(false);
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(DatasetInfo.newBuilder(tableId.getDataset()).setLocation("US").build());
        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldUpsertWithRetries() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getDatasetLabels()).thenReturn(Collections.emptyMap());
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        BigQueryTableDefinition bqDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition standardTableDefinition = bqDefinition.getTableDefinition(bqSchema);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBigQueryTableDefinition = bqDefinition.getTableDefinition(Schema.of(updatedBQSchemaFields));

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBigQueryTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLabels()).thenReturn(new HashMap<String, String>() {{
            put("new_key", "new_value");
        }});
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(bigquery.update(tableInfo))
                .thenThrow(new BigQueryException(500, " Error while updating bigquery table on callback:Exceeded rate limits: too many table update operations"))
                .thenThrow(new BigQueryException(500, " Error while updating bigquery table on callback:Exceeded rate limits: too many table update operations"))
                .thenThrow(new BigQueryException(500, " Error while updating bigquery table on callback:Exceeded rate limits: too many table update operations"))
                .thenReturn(table);
        bqClient.upsertTable(updatedBQSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, times(4)).update(tableInfo);
    }

    @Test
    public void shouldNotUpdateTableIfTableAlreadyExistsWithSameSchema() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(table.exists()).thenReturn(true);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldUpdateTableIfTableAlreadyExistsAndSchemaChanges() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        BigQueryTableDefinition bqDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition standardTableDefinition = bqDefinition.getTableDefinition(bqSchema);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBigQueryTableDefinition = bqDefinition.getTableDefinition(Schema.of(updatedBQSchemaFields));

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBigQueryTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(bigquery.update(tableInfo)).thenReturn(table);

        bqClient.upsertTable(updatedBQSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    @Test
    public void shouldUpdateTableIfTableNeedsToSetPartitionExpiry() {
        long partitionExpiry = 5184000000L;
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.getTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {
            {
                add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
                add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE)
                        .build());
                add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
                add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
                add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
                add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
                add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            }
        };

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig)
                .getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
        when(mockTimePartitioning.getExpirationMs()).thenReturn(null);
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    @Test(expected = BigQueryException.class)
    public void shouldThrowExceptionIfUpdateTableFails() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        BigQueryTableDefinition bqDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition standardTableDefinition = bqDefinition.getTableDefinition(bqSchema);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBigQueryTableDefinition = bqDefinition.getTableDefinition(Schema.of(updatedBQSchemaFields));

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBigQueryTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(bigquery.update(tableInfo)).thenThrow(new BigQueryException(404, "Failed to update"));

        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);
        bqClient.upsertTable(updatedBQSchemaFields);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDatasetLocationIsChanged() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("new-location");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldCreateBigQueryTableWithPartitionAndClustering() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(-1L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("string_field", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);
        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldCreateBigQueryTableWithPartitionOnly() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(-1L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldCreateBigQueryTableWithClusteringOnly() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("string_field", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);
        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldCreateBigQueryTableWithoutPartitionAndClustering() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(false);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldModifyClusteringColumnsFromExistingClusteredTable() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getClustering()).thenReturn(mockClustering);
        when(mockClustering.getFields()).thenReturn(Collections.singletonList("id"));

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    @Test
    public void shouldAddClusteringColumnsFromExistingUnPartitionedAndUnClusteredTable() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(true);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getClustering()).thenReturn(null);
        when(mockTableDefinition.getTimePartitioning()).thenReturn(null);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    @Test
    public void shouldAddClusteringColumnsFromExistingPartitionedAndUnClusteredTable() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(true);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
        when(mockTableDefinition.getClustering()).thenReturn(null);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    @Test
    public void shouldNotModifyClusteringColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(true);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getClustering()).thenReturn(mockClustering);
        when(mockClustering.getFields()).thenReturn(Arrays.asList("id", "city"));

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
        verify(instrumentation, times(1)).logInfo("Skipping bigquery table update, since proto schema hasn't changed");
    }
}
