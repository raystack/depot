package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.bigquery.exception.BQClusteringKeysException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class BigQueryTableDefinitionTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    @Mock
    private BigQuerySinkConfig bqConfig;

    @Before
    public void setup() {
        bqConfig = Mockito.mock(BigQuerySinkConfig.class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenSchemaIsNull() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionForRangePartition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("int_field");

        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowErrorIfPartitionFieldNotSet() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    @Test
    public void shouldReturnTimePartitioningWithPartitionExpiry() {
        long partitionExpiry = 5184000000L;
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        assertEquals("timestamp_field", tableDefinition.getTimePartitioning().getField());
        assertEquals(partitionExpiry, tableDefinition.getTimePartitioning().getExpirationMs().longValue());
    }

    @Test
    public void shouldReturnClusteringWithSingleColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        List<String> expectedColumns = Collections.singletonList("string_field");
        assertEquals(expectedColumns, tableDefinition.getClustering().getFields());
    }

    @Test
    public void shouldReturnClusteringWithMultipleColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING),
                Field.of("int_field", LegacySQLTypeName.INTEGER),
                Field.of("bool_field", LegacySQLTypeName.BOOLEAN),
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        List<String> expectedColumns = Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field");
        assertEquals(expectedColumns, tableDefinition.getClustering().getFields());
    }

    @Test
    public void shouldThrowExceptionIfClusteringKeyIsNotSet() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("Clustering key not specified for the table: table_name");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableName()).thenReturn("table_name");

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    @Test
    public void shouldThrowExceptionIfClusteringKeyIsSetMoreThanFour() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("Max number of columns for clustering is 4");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field", "another_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING),
                Field.of("int_field", LegacySQLTypeName.INTEGER),
                Field.of("bool_field", LegacySQLTypeName.BOOLEAN),
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("another_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    @Test
    public void shouldThrowExceptionIfClusteringKeyNotExistInSchema() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("One or more column names specified [string_field] not exist on the schema or a nested type which is not supported for clustering");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field2", LegacySQLTypeName.STRING),
                Field.of("int_field2", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    @Test
    public void shouldReturnPartitionedAndClusteredTableDefinition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("timestamp_field")
                .build();

        Clustering clustering = Clustering.newBuilder()
                .setFields(Collections.singletonList("string_field"))
                .build();

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNotNull(returnedPartitioning);
        assertEquals(returnedPartitioning.getField(), partitioning.getField());
        assertNotNull(returnedClustering);
        assertEquals(returnedClustering.getFields(), clustering.getFields());
    }

    @Test
    public void shouldReturnPartitionedTableWithoutClusteredTableDefinition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(false);

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("timestamp_field")
                .build();

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNotNull(returnedPartitioning);
        assertEquals(returnedPartitioning.getField(), partitioning.getField());
        assertNull(returnedClustering);
    }

    @Test
    public void shouldReturnClusteredTableWithoutPartitionTableDefinition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        Clustering clustering = Clustering.newBuilder()
                .setFields(Collections.singletonList("string_field"))
                .build();

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNull(returnedPartitioning);
        assertNotNull(returnedClustering);
        assertEquals(returnedClustering.getFields(), clustering.getFields());
    }

    @Test
    public void shouldReturnTableDefinitionWithoutPartitioningAndClustering() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(false);

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNull(returnedPartitioning);
        assertNull(returnedClustering);
    }
}
