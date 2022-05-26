package io.odpf.depot.bigquery.json;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.odpf.depot.bigquery.handler.BigQueryClient;
import io.odpf.depot.bigquery.handler.MessageRecordConverter;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import io.odpf.depot.config.BigQuerySinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BigqueryJsonUpdateListenerTest {

    private MessageRecordConverterCache converterCache;

    @Before
    public void setUp() throws Exception {

        converterCache = mock(MessageRecordConverterCache.class);
    }

    @Test
    public void shouldSetMessageRecordConverterAndUpsertTable() throws Exception {
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, Collections.emptyMap());
        BigQueryClient bqClient = mock(BigQueryClient.class);
        BigqueryJsonUpdateListener updateListener = new BigqueryJsonUpdateListener(bigQuerySinkConfig, converterCache, bqClient);
        updateListener.setOdpfMessageParser(null);
        updateListener.updateSchema();
        verify(converterCache, times(1)).setMessageRecordConverter(any(MessageRecordConverter.class));
        verify(bqClient, times(1)).upsertTable(Collections.emptyList());
    }

    @Test
    public void shouldCreateTableWithDefaultColumns() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "false"
        ));
        BigQueryClient mockBqClient = mock(BigQueryClient.class);
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.of("event_timestamp", LegacySQLTypeName.TIMESTAMP),
                Field.of("first_name", LegacySQLTypeName.STRING));
        verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
    }

    @Test
    public void shouldThrowErrorWhenFieldDataTypeMismatchWithCastToString() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "age=integer",
                "SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "true"
        ));
        BigQueryClient mockBqClient = mock(BigQueryClient.class);
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient);
        assertThrows(IllegalArgumentException.class, () -> bigqueryJsonUpdateListener.updateSchema());
    }

    @Test
    public void shouldNotCastPartitionKeyToString() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_BIGQUERY_TABLE_PARTITION_KEY", "event_timestamp",
                "SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true",
                "SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "true"
        ));
        BigQueryClient mockBqClient = mock(BigQueryClient.class);
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.of("event_timestamp", LegacySQLTypeName.TIMESTAMP),
                Field.of("first_name", LegacySQLTypeName.STRING));
        verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
    }

}
