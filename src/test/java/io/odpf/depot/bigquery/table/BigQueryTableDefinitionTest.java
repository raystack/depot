package io.odpf.depot.bigquery.table;

import com.google.cloud.bigquery.*;
import io.odpf.depot.config.BigQuerySinkConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class BigQueryTableDefinitionTest {

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
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field");

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(null);
    }

    @Test
    public void shouldReturnPartitionedAndClusteredTableDefinition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field");

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
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field");

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
