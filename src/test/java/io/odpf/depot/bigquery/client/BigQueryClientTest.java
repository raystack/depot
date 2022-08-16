package io.odpf.depot.bigquery.client;

import com.google.cloud.bigquery.*;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.metrics.BigQueryMetrics;
import io.odpf.depot.metrics.Instrumentation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
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

    private BigQueryClient bqClient;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private BigQueryMetrics metrics;

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

        StandardTableDefinition standardTableDefinition = getTableDefinition(bqSchemaFields);
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

        StandardTableDefinition standardTableDefinition = getTableDefinition(bqSchemaFields);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBQTableDefinition = getTableDefinition(updatedBQSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBQTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLabels()).thenReturn(new HashMap<String, String>() {{
            put("new_key", "new_value");
        }});
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
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

        StandardTableDefinition standardTableDefinition = getTableDefinition(bqSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(TableDefinition.Type.TABLE);
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
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

        StandardTableDefinition standardTableDefinition = getTableDefinition(bqSchemaFields);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBQTableDefinition = getTableDefinition(updatedBQSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBQTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
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
        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("partition_column")
                .setRequirePartitionFilter(true)
                .setExpirationMs(partitionExpiry)
                .build();
        StandardTableDefinition standardTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(bqSchema)
                .setTimePartitioning(partitioning)
                .build();

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(TableDefinition.Type.TABLE);
        when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
        when(mockTimePartitioning.getExpirationMs()).thenReturn(null);
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(table.exists()).thenReturn(true);

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

        StandardTableDefinition standardTableDefinition = getTableDefinition(bqSchemaFields);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBQTableDefinition = getTableDefinition(updatedBQSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBQTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
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

        StandardTableDefinition standardTableDefinition = getTableDefinition(bqSchemaFields);
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
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(0L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("string_field", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        Schema bqSchema = Schema.of(bqSchemaFields);
        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("timestamp_field")
                .setRequirePartitionFilter(true)
                .setExpirationMs(0L)
                .build();
        Clustering clustering = Clustering.newBuilder()
                .setFields(Collections.singletonList("string_field"))
                .build();
        StandardTableDefinition standardTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(bqSchema)
                .setTimePartitioning(partitioning)
                .setClustering(clustering)
                .build();

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
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(0L);
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
        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("partition_column")
                .setRequirePartitionFilter(true)
                .setExpirationMs(0L)
                .build();
        StandardTableDefinition standardTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(bqSchema)
                .setTimePartitioning(partitioning)
                .build();

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
        Clustering clustering = Clustering.newBuilder()
                .setFields(Collections.singletonList("string_field"))
                .build();
        StandardTableDefinition standardTableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(bqSchema)
                .setClustering(clustering)
                .build();

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

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, getTableDefinition(bqSchemaFields)).build();

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

    private StandardTableDefinition getTableDefinition(ArrayList<Field> bqSchemaFields) {
        Schema schema = Schema.of(bqSchemaFields);

        return StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
    }
}
