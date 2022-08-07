package io.odpf.depot.bigquery.table;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.depot.config.BigQuerySinkConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class BigQueryTablePartitioningTest {

    @Mock
    private BigQuerySinkConfig bqConfig;

    @Before
    public void setup() {
        bqConfig = Mockito.mock(BigQuerySinkConfig.class);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionForRangePartition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("int_field");

        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BigQueryTablePartitioning bigQueryTablePartitioning = new BigQueryTablePartitioning(bqConfig);
        bigQueryTablePartitioning.getPartitionedTableDefinition(bqSchema);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowErrorIfPartitionFieldNotSet() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BigQueryTablePartitioning bigQueryTablePartitioning = new BigQueryTablePartitioning(bqConfig);
        bigQueryTablePartitioning.getPartitionedTableDefinition(bqSchema);
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

        BigQueryTablePartitioning bigQueryTablePartitioning = new BigQueryTablePartitioning(bqConfig);
        TimePartitioning timePartitioning = bigQueryTablePartitioning.getPartitionedTableDefinition(bqSchema);

        assertEquals("timestamp_field", timePartitioning.getField());
        assertEquals(partitionExpiry, timePartitioning.getExpirationMs().longValue());
    }

    @Test
    public void shouldReturnTimePartitioningWithNullPartitionExpiryIfLessThanZero() {
        long partitionExpiry = -1L;
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BigQueryTablePartitioning bigQueryTablePartitioning = new BigQueryTablePartitioning(bqConfig);
        TimePartitioning timePartitioning = bigQueryTablePartitioning.getPartitionedTableDefinition(bqSchema);

        assertEquals("timestamp_field", timePartitioning.getField());
        assertNull(timePartitioning.getExpirationMs());
    }

    @Test
    public void shouldReturnTimePartitioningWithNullPartitionExpiryIfEqualsZero() {
        long partitionExpiry = 0L;
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BigQueryTablePartitioning bigQueryTablePartitioning = new BigQueryTablePartitioning(bqConfig);
        TimePartitioning timePartitioning = bigQueryTablePartitioning.getPartitionedTableDefinition(bqSchema);

        assertEquals("timestamp_field", timePartitioning.getField());
        assertNull(timePartitioning.getExpirationMs());
    }
}
