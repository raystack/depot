package io.odpf.depot.bigquery.json;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.odpf.depot.bigquery.handler.BigQueryClient;
import io.odpf.depot.bigquery.handler.MessageRecordConverter;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import io.odpf.depot.config.BigQuerySinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigqueryJsonUpdateListenerTest {

    private MessageRecordConverterCache converterCache;
    private BigQueryClient mockBqClient;

    @Before
    public void setUp() throws Exception {
        converterCache = mock(MessageRecordConverterCache.class);
        mockBqClient = mock(BigQueryClient.class);
        Schema emptySchema = Schema.of();
        when(mockBqClient.getSchema()).thenReturn(emptySchema);
    }

    @Test
    public void shouldSetMessageRecordConverterAndUpsertTable() throws Exception {
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, Collections.emptyMap());
        BigqueryJsonUpdateListener updateListener = new BigqueryJsonUpdateListener(bigQuerySinkConfig, converterCache, mockBqClient);
        updateListener.setOdpfMessageParser(null);
        updateListener.updateSchema();
        verify(converterCache, times(1)).setMessageRecordConverter(any(MessageRecordConverter.class));
        verify(mockBqClient, times(1)).upsertTable(Collections.emptyList());
    }

    @Test
    public void shouldCreateTableWithDefaultColumns() {

        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "false"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.of("event_timestamp", LegacySQLTypeName.TIMESTAMP),
                Field.of("first_name", LegacySQLTypeName.STRING));
        verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
    }

    @Test
    public void shouldCreateTableWithDefaultColumnsAndExistingTableColumns() {
        Field existingField1 = Field.of("existing_field1", LegacySQLTypeName.STRING);
        Field existingField2 = Field.of("existing_field2", LegacySQLTypeName.STRING);
        when(mockBqClient.getSchema()).thenReturn(Schema.of(existingField1,
                existingField2));
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "false"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient);
        bigqueryJsonUpdateListener.updateSchema();
        ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
        List<Field> actualFields = listArgumentCaptor.getValue();
        Field eventTimestampField = Field.of("event_timestamp", LegacySQLTypeName.TIMESTAMP);
        Field firstNameField = Field.of("first_name", LegacySQLTypeName.STRING);
        assertThat(actualFields, containsInAnyOrder(eventTimestampField, firstNameField, existingField1, existingField2));
    }

    @Test
    public void shouldThrowErrorWhenFieldDataTypeMismatchWithCastToString() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "age=integer",
                "SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "true"
        ));
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
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.of("event_timestamp", LegacySQLTypeName.TIMESTAMP),
                Field.of("first_name", LegacySQLTypeName.STRING));
        verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
    }

    @Test
    public void shouldThrowExceptionWhenDynamicSchemaNotEnabled() {
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class,
                ImmutableMap.of("SINK_CONNECTOR_SCHEMA_JSON_DYNAMIC_SCHEMA_ENABLE", "false"));
        assertThrows(UnsupportedOperationException.class,
                () -> new BigqueryJsonUpdateListener(bigQuerySinkConfig, converterCache, mockBqClient));

    }
}
