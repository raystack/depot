package io.odpf.depot.bigquery.handler;

import com.google.api.client.util.DateTime;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.odpf.depot.bigquery.client.BigQueryClient;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JsonErrorHandlerTest {

    private final Schema emptyTableSchema = Schema.of();

    private BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, Collections.emptyMap());

    @Mock
    private BigQueryClient bigQueryClient;

    @Captor
    private ArgumentCaptor<List<Field>> fieldsArgumentCaptor;

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void shouldUpdateTableFieldsOnSchemaError() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError bigQueryError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> insertErrors = ImmutableMap.of(0L, asList(bigQueryError));

        Record validRecord = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        List<Record> records = ImmutableList.of(validRecord);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);

        jsonErrorHandler.handle(insertErrors, records);
        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName));
    }

    @Test
    public void shouldNotUpdateTableWhenNoSchemaError() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError serverError = new BigQueryError("otherresons", "planet eart", "server error");
        BigQueryError anotherError = new BigQueryError("otherresons", "planet eart", "server error");
        Map<Long, List<BigQueryError>> insertErrors = ImmutableMap.of(0L, asList(serverError, anotherError));

        Record validRecord = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        List<Record> records = ImmutableList.of(validRecord);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(insertErrors, records);

        verify(bigQueryClient, never()).upsertTable(any());

    }

    @Test
    public void shouldUpdateTableFieldsForMultipleRecords() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError firstNameNotFoundError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        BigQueryError anotherError = new BigQueryError("otherresons", "planet eart", "some error");
        BigQueryError lastNameNotFoundError = new BigQueryError("invalid", "first_name", "no such field: last_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, asList(firstNameNotFoundError, anotherError),
                1L, asList(lastNameNotFoundError));


        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Map<String, Object> columnsMapWithLastName = ImmutableMap.of("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithLastName)
                .build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);


        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);
        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName));
    }

    @Test
    public void shouldIngoreRecordsWhichHaveOtherErrors() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        BigQueryError otherError = new BigQueryError("otherresons", "planet eart", "server error");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                1L, asList(noSuchFieldError, otherError),
                0L, asList(otherError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("last_name", "john carmack"))
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(lastName));
    }

    @Test
    public void shouldIngoreRecordsWithNoErrors() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(1L, asList(noSuchFieldError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("last_name", "john carmack"))
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(lastName));
    }

    @Test
    public void shouldUpdateOnlyUniqueFields() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
        0L, asList(noSuchFieldError),
        1L, asList(noSuchFieldError),
        2L, asList(noSuchFieldError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Map<String, Object> columnsMapWithLastName = ImmutableMap.of("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithLastName)
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(columnsMapWithLastName)
                .build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName));
    }

    @Test
    public void shouldUpdatWithBothMissingFieldsAndExistingTableFields() {
        //existing table fields
        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
        0L, asList(noSuchFieldError),
        1L, asList(noSuchFieldError),
        2L, asList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
        "first_name", "john doe",
        "newFieldAddress", "planet earth");
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("newFieldDog", "golden retriever"))
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("newFieldDog", "golden retriever"))
                .build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = Field.of("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = Field.of("newFieldAddress", LegacySQLTypeName.STRING);

        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName, newFieldDog, newFieldAddress));
    }

    @Test
    public void shouldUpsertTableWithPartitionKey() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
        0L, asList(noSuchFieldError),
        1L, asList(noSuchFieldError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Map<String, Object> columnsMapWithTimestamp = ImmutableMap.of(
        "last_name", "john carmack",
        "event_timestamp_partition", "today's date");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithTimestamp).build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName);

        Map<String, String> envMap = ImmutableMap.of(
            "SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true",
            "SINK_BIGQUERY_TABLE_PARTITION_KEY", "event_timestamp_partition",
            "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "event_timestamp_partition=timestamp");
        BigQuerySinkConfig partitionKeyConfig = ConfigFactory.create(BigQuerySinkConfig.class, envMap);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, partitionKeyConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);


        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);
        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        Field eventTimestamp = Field.of("event_timestamp_partition", LegacySQLTypeName.TIMESTAMP);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName, eventTimestamp));
    }

    @Test
    public void shouldThrowExceptionWhenCastFieldsToStringNotTrue() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(0L, asList(noSuchFieldError));

        Record validRecord = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        List<Record> records = asList(validRecord);
        BigQuerySinkConfig stringDisableConfig = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
            "SINK_CONNECTOR_SCHEMA_JSON_OUTPUT_DEFAULT_DATATYPE_STRING_ENABLE", "false"));
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, stringDisableConfig, instrumentation);
        assertThrows(UnsupportedOperationException.class, () -> {
            jsonErrorHandler.handle(errorInfoMap, records);
        });

        verify(bigQueryClient, never()).upsertTable((List<Field>) any());
    }

    @Test
    public void shouldUpdateMissingMetadataFields() {
        //existing table fields
        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
        0L, asList(noSuchFieldError),
        1L, asList(noSuchFieldError),
        2L, asList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
        "first_name", "john doe",
        "newFieldAddress", "planet earth",
        "message_offset", 111,
        "load_time", new DateTime(System.currentTimeMillis()));
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .metadata(ImmutableMap.of(
                        "message_offset", 111,
                        "load_time", new DateTime(System.currentTimeMillis())))
                .build();

        Map<String, Object> columnsMapWithNewFieldDog = ImmutableMap.of(
        "newFieldDog", "golden retriever",
        "load_time", new DateTime(System.currentTimeMillis()),
        "message_offset", 11);
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .metadata(ImmutableMap.of(
                        "load_time", new DateTime(System.currentTimeMillis()),
                        "message_offset", 11))
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        Map<String, String> config = ImmutableMap.of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,load_time=timestamp");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = Field.of("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = Field.of("newFieldAddress", LegacySQLTypeName.STRING);

        Field messageOffset = Field.of("message_offset", LegacySQLTypeName.INTEGER);
        Field loadTime = Field.of("load_time", LegacySQLTypeName.TIMESTAMP);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields,
                containsInAnyOrder(messageOffset, loadTime, firstName, lastName, newFieldDog, newFieldAddress));
    }

    @Test
    public void shouldUpdateMissingMetadataFieldsAndDefaultColumns() {
        //existing table fields
        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, asList(noSuchFieldError),
                1L, asList(noSuchFieldError),
                2L, asList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
                "first_name", "john doe",
                "newFieldAddress", "planet earth",
                "depot", 123,
                "message_offset", 111,
                "load_time", new DateTime(System.currentTimeMillis()));
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .metadata(ImmutableMap.of(
                        "message_offset", 111,
                        "load_time", new DateTime(System.currentTimeMillis())))
                .build();

        Map<String, Object> columnsMapWithNewFieldDog = ImmutableMap.of(
                "newFieldDog", "golden retriever",
                "load_time", new DateTime(System.currentTimeMillis()),
                "message_offset", 11);
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .metadata(ImmutableMap.of(
                        "load_time", new DateTime(System.currentTimeMillis()),
                        "message_offset", 11))
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        Map<String, String> config = ImmutableMap.of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,load_time=timestamp",
                "SINK_BIGQUERY_SCHEMA_JSON_OUTPUT_DEFAULT_COLUMNS", "event_timestamp_partition=timestamp,depot=integer");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = Field.of("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = Field.of("newFieldAddress", LegacySQLTypeName.STRING);

        Field messageOffset = Field.of("message_offset", LegacySQLTypeName.INTEGER);
        Field loadTime = Field.of("load_time", LegacySQLTypeName.TIMESTAMP);
        Field depot = Field.of("depot", LegacySQLTypeName.INTEGER);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields,
                containsInAnyOrder(messageOffset, loadTime, firstName, lastName, newFieldDog, newFieldAddress, depot));
    }

    @Test
    public void shouldNotAddMetadataFieldsWhenDisabled() {
        //existing table fields
        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, asList(noSuchFieldError),
                1L, asList(noSuchFieldError),
                2L, asList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
                "first_name", "john doe",
                "newFieldAddress", "planet earth");
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .metadata(ImmutableMap.of(
                        "message_offset", 111,
                        "load_time", new DateTime(System.currentTimeMillis())))
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of(
                        "newFieldDog", "golden retriever"))
                .metadata(ImmutableMap.of(
                        "load_time", new DateTime(System.currentTimeMillis()),
                        "message_offset", 11))
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of(
                        "newFieldDog", "german sheppperd"))
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        Map<String, String> config = ImmutableMap
                .of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,load_time=timestamp",
                        "SINK_BIGQUERY_ADD_METADATA_ENABLED", "false");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = Field.of("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = Field.of("newFieldAddress", LegacySQLTypeName.STRING);

        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields,
                containsInAnyOrder(firstName, lastName, newFieldDog, newFieldAddress));
    }

    @Test
    public void shouldThrowErrorForNamespacedMetadataNotSupported() {
        //existing table fields
        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, asList(noSuchFieldError),
                1L, asList(noSuchFieldError),
                2L, asList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
        "first_name", "john doe",
        "newFieldAddress", "planet earth",
        "message_offset", 111);
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .build();

        Map<String, Object> columnsMapWithNewFieldDog = ImmutableMap.of(
        "newFieldDog", "golden retriever",
        "load_time", new DateTime(System.currentTimeMillis()));
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        Map<String, String> config = ImmutableMap.of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,load_time=timestamp",
                "SINK_BIGQUERY_METADATA_NAMESPACE", "hello_world_namespace");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        assertThrows(UnsupportedOperationException.class, () -> {
            jsonErrorHandler.handle(errorInfoMap, validRecords);
        });
        verify(bigQueryClient, never()).upsertTable(any());
    }
}

