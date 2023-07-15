package org.raystack.depot.bigquery.json;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.bigquery.converter.MessageRecordConverter;
import org.raystack.depot.bigquery.converter.MessageRecordConverterCache;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.metrics.Instrumentation;
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
        private Instrumentation instrumentation;

        @Before
        public void setUp() throws Exception {
                converterCache = mock(MessageRecordConverterCache.class);
                mockBqClient = mock(BigQueryClient.class);
                Schema emptySchema = Schema.of();
                when(mockBqClient.getSchema()).thenReturn(emptySchema);
                instrumentation = mock(Instrumentation.class);
        }

        @Test
        public void shouldSetMessageRecordConverterAndUpsertTable() {
                BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class,
                                Collections.emptyMap());
                BigqueryJsonUpdateListener updateListener = new BigqueryJsonUpdateListener(bigQuerySinkConfig,
                                converterCache, mockBqClient, instrumentation);
                updateListener.setOdpfMessageParser(null);
                updateListener.updateSchema();
                verify(converterCache, times(1)).setMessageRecordConverter(any(MessageRecordConverter.class));
                verify(mockBqClient, times(1)).upsertTable(Collections.emptyList());
        }

        @Test
        public void shouldCreateTableWithDefaultColumns() {

                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                                "SINK_CONNECTOR_DEFAULT_DATATYPE_STRING_ENABLE", "false"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                bigqueryJsonUpdateListener.updateSchema();
                List<Field> bqSchemaFields = ImmutableList.of(
                                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP)
                                                .setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE)
                                                .build());
                verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
        }

        @Test
        public void shouldCreateTableWithDefaultColumnsAndMetadataFields() {
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false",
                                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                                "message_offset=integer,message_topic=string,message_timestamp=timestamp",
                                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                bigqueryJsonUpdateListener.updateSchema();
                List<Field> bqSchemaFields = ImmutableList.of(
                                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP)
                                                .setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE)
                                                .build(),
                                Field.newBuilder("message_offset", LegacySQLTypeName.INTEGER)
                                                .setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("message_topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE)
                                                .build(),
                                Field.newBuilder("message_timestamp", LegacySQLTypeName.TIMESTAMP)
                                                .setMode(Field.Mode.NULLABLE).build());
                ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
                verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
                assertThat(listArgumentCaptor.getValue(), containsInAnyOrder(bqSchemaFields.toArray()));
        }

        @Test
        public void shouldCreateTableWithDefaultColumnsWithDdifferentTypesAndMetadataFields() {
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=integer",
                                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true",
                                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                                "message_offset=integer,message_topic=string,message_timestamp=timestamp",
                                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                bigqueryJsonUpdateListener.updateSchema();
                List<Field> bqSchemaFields = ImmutableList.of(
                                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP)
                                                .setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("first_name", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE)
                                                .build(),
                                Field.newBuilder("message_offset", LegacySQLTypeName.INTEGER)
                                                .setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("message_topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE)
                                                .build(),
                                Field.newBuilder("message_timestamp", LegacySQLTypeName.TIMESTAMP)
                                                .setMode(Field.Mode.NULLABLE).build());
                ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
                verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
                assertThat(listArgumentCaptor.getValue(), containsInAnyOrder(bqSchemaFields.toArray()));
        }

        @Test
        public void shouldNotAddMetadataFields() {
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false",
                                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                                "message_offset=integer,message_topic=string,message_timestamp=timestamp",
                                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "false"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                bigqueryJsonUpdateListener.updateSchema();
                List<Field> bqSchemaFields = ImmutableList.of(
                                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP)
                                                .setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE)
                                                .build());
                ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
                verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
                assertThat(listArgumentCaptor.getValue(), containsInAnyOrder(bqSchemaFields.toArray()));
        }

        @Test
        public void shouldThrowErrorIfDefaultColumnsAndMetadataFieldsContainSameEntryCalledFirstName() {
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false",
                                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,first_name=integer",
                                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                assertThrows(IllegalArgumentException.class, bigqueryJsonUpdateListener::updateSchema);
        }

        @Test
        public void shouldThrowErrorIfMetadataNamespaceIsNotEmpty() {
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,first_name=integer",
                                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true",
                                "SINK_BIGQUERY_METADATA_NAMESPACE", "metadata_namespace"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                assertThrows(UnsupportedOperationException.class, bigqueryJsonUpdateListener::updateSchema);
        }

        @Test
        public void shouldCreateTableWithDefaultColumnsAndExistingTableColumns() {
                Field existingField1 = Field.of("existing_field1", LegacySQLTypeName.STRING);
                Field existingField2 = Field.of("existing_field2", LegacySQLTypeName.STRING);
                when(mockBqClient.getSchema()).thenReturn(Schema.of(existingField1,
                                existingField2));
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                bigqueryJsonUpdateListener.updateSchema();
                ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
                verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
                List<Field> actualFields = listArgumentCaptor.getValue();
                Field eventTimestampField = Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP)
                                .setMode(Field.Mode.NULLABLE).build();
                Field firstNameField = Field.newBuilder("first_name", LegacySQLTypeName.STRING)
                                .setMode(Field.Mode.NULLABLE).build();
                assertThat(actualFields, containsInAnyOrder(eventTimestampField, firstNameField, existingField1,
                                existingField2));
        }

        @Test
        public void shouldNotCastPartitionKeyToString() {
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                                "SINK_BIGQUERY_TABLE_PARTITION_KEY", "event_timestamp",
                                "SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true",
                                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                bigqueryJsonUpdateListener.updateSchema();
                List<Field> bqSchemaFields = ImmutableList.of(
                                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP)
                                                .setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE)
                                                .build());
                verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
        }

        @Test
        public void shouldThrowErrorWhenPartitionKeyTypeIsNotCorrect() {
                BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=integer,first_name=string",
                                "SINK_BIGQUERY_TABLE_PARTITION_KEY", "event_timestamp",
                                "SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true",
                                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true"));
                BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config,
                                converterCache, mockBqClient, instrumentation);
                assertThrows(UnsupportedOperationException.class, bigqueryJsonUpdateListener::updateSchema);
        }

        @Test
        public void shouldThrowExceptionWhenDynamicSchemaNotEnabled() {
                BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class,
                                ImmutableMap.of("SINK_BIGQUERY_DYNAMIC_SCHEMA_ENABLE", "false"));
                assertThrows(UnsupportedOperationException.class,
                                () -> new BigqueryJsonUpdateListener(bigQuerySinkConfig, converterCache, mockBqClient,
                                                instrumentation));

        }
}
