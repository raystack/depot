package io.odpf.sink.connectors.bigquery.handler;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import io.odpf.sink.connectors.bigquery.handler.BigQueryClient;
import io.odpf.sink.connectors.bigquery.handler.JsonErrorHandler;
import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.error.ErrorInfo;
import io.odpf.sink.connectors.error.ErrorType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JsonErrorHandlerTest {

    private final Schema emptyTableSchema = Schema.of();
    private final ErrorInfo.ErrorInfoBuilder errorBuilder = ErrorInfo.builder();

    @Mock
    private BigQueryClient bigQueryClient;

    @Captor
    private ArgumentCaptor<List<Field>> fieldsArgumentCaptor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void shouldUpdateTableFieldsOnSchemaError() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        errorInfoMap.put(0L, unknowFieldError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecord = Record.builder().columns(columnsMap).build();

        List<Record> records = new ArrayList<>();
        records.add(validRecord);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, true);
        jsonErrorHandler.handle(errorInfoMap, records);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field firstName = Field.of("first_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName));
    }

    @Test
    public void shouldNotUpdateTableWhenNoSchemaError() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo sinkError = errorBuilder.errorType(ErrorType.SINK_4XX_ERROR).build();
        errorInfoMap.put(0L, sinkError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecord = Record.builder().columns(columnsMap).build();

        List<Record> records = new ArrayList<>();
        records.add(validRecord);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, true);
        jsonErrorHandler.handle(errorInfoMap, records);

        verify(bigQueryClient, never()).upsertTable(any());

    }

    @Test
    public void shouldUpdateTableFieldsForMultipleRecords() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        errorInfoMap.put(0L, unknowFieldError);
        errorInfoMap.put(1L, unknowFieldError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecordWithFirstName = Record.builder().columns(columnsMap).build();

        Map<String, Object> columnsMapWithLastName = new HashMap<>();
        columnsMapWithLastName.put("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithLastName).build();

        List<Record> validRecords = new ArrayList<>();
        validRecords.add(validRecordWithFirstName);
        validRecords.add(validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, true);
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
        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        ErrorInfo sinkError = errorBuilder.errorType(ErrorType.SINK_4XX_ERROR).build();
        errorInfoMap.put(1L, unknowFieldError);
        errorInfoMap.put(0L, sinkError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecordWithFirstName = Record.builder().columns(columnsMap).build();

        Map<String, Object> columnsMapWithLastName = new HashMap<>();
        columnsMapWithLastName.put("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithLastName).build();

        List<Record> validRecords = new ArrayList<>();
        validRecords.add(validRecordWithFirstName);
        validRecords.add(validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, true);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(lastName));
    }

    @Test
    public void shouldIngoreRecordsWithNoErrors() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        errorInfoMap.put(1L, unknowFieldError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecordWithFirstName = Record.builder().columns(columnsMap).build();

        Map<String, Object> columnsMapWithLastName = new HashMap<>();
        columnsMapWithLastName.put("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithLastName).build();

        List<Record> validRecords = new ArrayList<>();
        validRecords.add(validRecordWithFirstName);
        validRecords.add(validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, true);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        Field lastName = Field.of("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(lastName));
    }

    @Test
    public void shouldUpdateOnlyUniqueFields() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        errorInfoMap.put(0L, unknowFieldError);
        errorInfoMap.put(1L, unknowFieldError);
        errorInfoMap.put(2L, unknowFieldError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecordWithFirstName = Record.builder().columns(columnsMap).build();

        Map<String, Object> columnsMapWithLastName = new HashMap<>();
        columnsMapWithLastName.put("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithLastName).build();
        Record anotheRecordWithLastName = Record.builder().columns(columnsMapWithLastName).build();

        List<Record> validRecords = new ArrayList<>();
        validRecords.add(validRecordWithFirstName);
        validRecords.add(validRecordWithLastName);
        validRecords.add(anotheRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, true);
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

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        errorInfoMap.put(0L, unknowFieldError);
        errorInfoMap.put(1L, unknowFieldError);
        errorInfoMap.put(2L, unknowFieldError);

        Map<String, Object> columnsMapWithFistName = new HashMap<>();
        columnsMapWithFistName.put("first_name", "john doe");
        columnsMapWithFistName.put("newFieldAddress", "planet earth");
        Record validRecordWithFirstName = Record.builder().columns(columnsMapWithFistName).build();

        Map<String, Object> columnsMapWithNewFieldDog = new HashMap<>();
        columnsMapWithNewFieldDog.put("newFieldDog", "golden retriever");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithNewFieldDog).build();
        Record anotheRecordWithLastName = Record.builder().columns(columnsMapWithNewFieldDog).build();

        List<Record> validRecords = new ArrayList<>();
        validRecords.add(validRecordWithFirstName);
        validRecords.add(validRecordWithLastName);
        validRecords.add(anotheRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, true);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable((List<Field>) fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = Field.of("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = Field.of("newFieldAddress", LegacySQLTypeName.STRING);

        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName, newFieldDog, newFieldAddress));
    }

    //TODO upsert event_timestamp fields

    @Test
    public void shouldUpsertTableWithPartitionKeyTimestampField() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        errorInfoMap.put(0L, unknowFieldError);
        errorInfoMap.put(1L, unknowFieldError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecordWithFirstName = Record.builder().columns(columnsMap).build();

        Map<String, Object> columnsMapWithTimestamp = new HashMap<>();
        columnsMapWithTimestamp.put("last_name", "john carmack");
        columnsMapWithTimestamp.put("event_timestamp_partition", "today's date");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithTimestamp).build();

        List<Record> validRecords = new ArrayList<>();
        validRecords.add(validRecordWithFirstName);
        validRecords.add(validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, "event_timestamp_partition", true);
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

        Map<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        ErrorInfo unknowFieldError = errorBuilder.errorType(ErrorType.UNKNOWN_FIELDS_ERROR).build();
        errorInfoMap.put(0L, unknowFieldError);

        Map<String, Object> columnsMap = new HashMap<>();
        columnsMap.put("first_name", "john doe");
        Record validRecord = Record.builder().columns(columnsMap).build();

        List<Record> records = new ArrayList<>();
        records.add(validRecord);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, null, false);
        assertThrows(UnsupportedOperationException.class, () -> {
            jsonErrorHandler.handle(errorInfoMap, records);
        });

        verify(bigQueryClient, never()).upsertTable((List<Field>) any());
    }
}

