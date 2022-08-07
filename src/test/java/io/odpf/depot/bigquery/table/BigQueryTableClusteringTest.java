package io.odpf.depot.bigquery.table;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import io.odpf.depot.bigquery.exception.BQClusteringKeysException;
import io.odpf.depot.config.BigQuerySinkConfig;
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
import static org.mockito.Mockito.when;

public class BigQueryTableClusteringTest {

    @Mock
    private BigQuerySinkConfig bqConfig;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setup() {
        bqConfig = Mockito.mock(BigQuerySinkConfig.class);
    }

    @Test
    public void shouldReturnClusteringWithSingleColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field");

        Schema schema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableClustering bigQueryTableClustering = new BigQueryTableClustering(bqConfig);
        Clustering clustering = bigQueryTableClustering.getClusteredTableDefinition(schema);

        List<String> expectedColumns = Collections.singletonList("string_field");
        assertEquals(expectedColumns, clustering.getFields());
    }

    @Test
    public void shouldReturnClusteringWithMultipleColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field,int_field,bool_field,timestamp_field");

        Schema schema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING),
                Field.of("int_field", LegacySQLTypeName.INTEGER),
                Field.of("bool_field", LegacySQLTypeName.BOOLEAN),
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP)
        );

        BigQueryTableClustering bigQueryTableClustering = new BigQueryTableClustering(bqConfig);
        Clustering clustering = bigQueryTableClustering.getClusteredTableDefinition(schema);

        List<String> expectedColumns = Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field");
        assertEquals(expectedColumns, clustering.getFields());
    }

    @Test
    public void shouldTrimClusteringKeyConfig() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field   ,int_field,    bool_field, timestamp_field ");

        Schema schema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING),
                Field.of("int_field", LegacySQLTypeName.INTEGER),
                Field.of("bool_field", LegacySQLTypeName.BOOLEAN),
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP)
        );

        BigQueryTableClustering bigQueryTableClustering = new BigQueryTableClustering(bqConfig);
        Clustering clustering = bigQueryTableClustering.getClusteredTableDefinition(schema);

        List<String> expectedColumns = Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field");
        assertEquals(expectedColumns, clustering.getFields());
    }

    @Test
    public void shouldThrowExceptionIfClusteringKeyIsNotSet() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("Clustering key not specified for the table: table_name");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableName()).thenReturn("table_name");

        Schema schema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableClustering bigQueryTableClustering = new BigQueryTableClustering(bqConfig);
        bigQueryTableClustering.getClusteredTableDefinition(schema);
    }

    @Test
    public void shouldThrowExceptionIfClusteringKeyIsSetMoreThanFour() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("Max number of columns for clustering is 4");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field,int_field,bool_field,timestamp_field,another_field");

        Schema schema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING),
                Field.of("int_field", LegacySQLTypeName.INTEGER),
                Field.of("bool_field", LegacySQLTypeName.BOOLEAN),
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("another_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableClustering bigQueryTableClustering = new BigQueryTableClustering(bqConfig);
        bigQueryTableClustering.getClusteredTableDefinition(schema);
    }

    @Test
    public void shouldThrowExceptionIfClusteringKeyNotExistInSchema() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("One or more column names specified [string_field] not exist on the schema or a nested type which is not supported for clustering");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKey()).thenReturn("string_field");

        Schema schema = Schema.of(
                Field.of("string_field2", LegacySQLTypeName.STRING),
                Field.of("int_field2", LegacySQLTypeName.STRING)
        );

        BigQueryTableClustering bigQueryTableClustering = new BigQueryTableClustering(bqConfig);
        bigQueryTableClustering.getClusteredTableDefinition(schema);
    }
}
