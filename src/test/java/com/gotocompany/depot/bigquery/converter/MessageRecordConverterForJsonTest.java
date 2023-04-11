package com.gotocompany.depot.bigquery.converter;

import com.google.common.collect.ImmutableMap;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.json.JsonMessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MessageRecordConverterForJsonTest {

    private static final TimeZone TZ = TimeZone.getTimeZone("UTC");
    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    static {
        DF.setTimeZone(TZ);
    }

    private final SinkConfig defaultConfig = ConfigFactory.create(SinkConfig.class, Collections.emptyMap());
    private final Record.RecordBuilder recordBuilder = Record.builder();
    private final Map<String, Object> emptyMetadata = Collections.emptyMap();
    private final Map<String, Object> emptyColumnsMap = Collections.emptyMap();
    private final ErrorInfo noError = null;
    private final Instrumentation instrumentation = mock(Instrumentation.class);
    private final JsonParserMetrics jsonParserMetrics = new JsonParserMetrics(defaultConfig);

    @Test
    public void shouldReturnEmptyRecordsForEmptyList() {
        MessageParser parser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        MessageRecordConverter converter = new MessageRecordConverter(parser, null);
        List<Message> emptyMessageList = Collections.emptyList();

        Records records = converter.convert(emptyMessageList);
        List<Record> emptyRecordList = Collections.emptyList();
        Records expectedRecords = new Records(emptyRecordList, emptyRecordList);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void shouldConvertJsonMessagesToRecordForLogMessage() {
        MessageParser parser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_MESSAGE");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig);
        List<Message> messages = new ArrayList<>();
        messages.add(getMessageForString("{ \"first_name\": \"john doe\"}"));
        messages.add(getMessageForString("{ \"last_name\": \"walker\"}"));

        Records records = converter.convert(messages);

        List<Record> expectedValidRecords = new ArrayList<>();

        Record validRecord1 = recordBuilder
                .metadata(emptyMetadata)
                .columns(ImmutableMap.of("first_name", "john doe"))
                .index(0L)
                .errorInfo(noError)
                .build();

        Record validRecord2 = recordBuilder
                .metadata(emptyMetadata)
                .columns(ImmutableMap.of("last_name", "walker"))
                .index(1L)
                .errorInfo(noError)
                .build();
        expectedValidRecords.add(validRecord1);
        expectedValidRecords.add(validRecord2);
        List<Record> invalidRecords = Collections.emptyList();
        Records expectedRecords = new Records(expectedValidRecords, invalidRecords);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void shouldConvertJsonMessagesToRecordForLogKey() {
        MessageParser parser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_KEY");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig);
        List<Message> messages = new ArrayList<>();
        messages.add(new Message("{ \"first_name\": \"john doe\"}".getBytes(), null));
        messages.add(new Message("{ \"last_name\": \"walker\"}".getBytes(), null));

        Records records = converter.convert(messages);

        List<Record> expectedValidRecords = new ArrayList<>();

        Record validRecord1 = recordBuilder
                .metadata(emptyMetadata)
                .columns(ImmutableMap.of("first_name", "john doe"))
                .index(0L)
                .errorInfo(noError)
                .build();

        Record validRecord2 = recordBuilder
                .metadata(emptyMetadata)
                .columns(ImmutableMap.of("last_name", "walker"))
                .index(1L)
                .errorInfo(noError)
                .build();
        expectedValidRecords.add(validRecord1);
        expectedValidRecords.add(validRecord2);
        List<Record> invalidRecords = Collections.emptyList();
        Records expectedRecords = new Records(expectedValidRecords, invalidRecords);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void shouldHandleBothInvalidAndValidJsonMessages() {
        MessageParser parser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_MESSAGE");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig);
        List<Message> messages = new ArrayList<>();
        messages.add(getMessageForString("{ \"first_name\": \"john doe\"}"));
        messages.add(getMessageForString("{ invalid json str"));
        messages.add(getMessageForString("{ \"last_name\": \"walker\"}"));
        messages.add(getMessageForString("another invalid message"));
        String nestedJsonStr = "{\n"
                + "  \"event_value\": {\n"
                + "    \"CustomerLatitude\": \"-6.166895595817224\",\n"
                + "    \"fb_content_type\": \"product\"\n"
                + "  },\n"
                + "  \"ip\": \"210.210.175.250\",\n"
                + "  \"oaid\": null,\n"
                + "  \"event_time\": \"2022-05-06 08:03:43.561\",\n"
                + "  \"is_receipt_validated\": null,\n"
                + "  \"contributor_1_campaign\": null\n"
                + "}";

        messages.add(getMessageForString(nestedJsonStr));
        Records records = converter.convert(messages);
        List<Record> expectedValidRecords = new ArrayList<>();
        Record validRecord1 = recordBuilder
                .metadata(emptyMetadata)
                .columns(ImmutableMap.of("first_name", "john doe"))
                .index(0L)
                .errorInfo(MessageRecordConverterForJsonTest.this.noError)
                .build();

        Record validRecord2 = recordBuilder
                .metadata(emptyMetadata)
                .columns(ImmutableMap.of("last_name", "walker"))
                .index(2L)
                .errorInfo(MessageRecordConverterForJsonTest.this.noError)
                .build();

        expectedValidRecords.add(validRecord1);
        expectedValidRecords.add(validRecord2);

        ErrorInfo errorInfo = new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR);
        ErrorInfo invalidMessageError = new ErrorInfo(null, ErrorType.INVALID_MESSAGE_ERROR);
        List<Record> expectedInvalidRecords = new ArrayList<>();
        Record.RecordBuilder invalidRecordBuilder = recordBuilder.metadata(emptyMetadata).columns(emptyColumnsMap);

        Record invalidRecord1 = invalidRecordBuilder
                .index(1L)
                .errorInfo(errorInfo)
                .build();

        Record invalidRecord3 = invalidRecordBuilder
                .index(3L)
                .errorInfo(errorInfo)
                .build();

        Record invalidRecord4 = invalidRecordBuilder
                .index(4L)
                .errorInfo(invalidMessageError)
                .build();

        expectedInvalidRecords.add(invalidRecord1);
        expectedInvalidRecords.add(invalidRecord3);
        expectedInvalidRecords.add(invalidRecord4);
        assertEquals(expectedValidRecords, records.getValidRecords());
        assertEquals(expectedInvalidRecords, records.getInvalidRecords());
    }

    @Test
    public void shouldInjectEventTimestamp() throws ParseException {
        MessageParser parser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        Map<String, String> configMap = ImmutableMap.of(
                "SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_MESSAGE",
                "SINK_CONNECTOR_SCHEMA_DATA_TYPE", "json",
                "SINK_BIGQUERY_ADD_EVENT_TIMESTAMP_ENABLE", "true");

        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig);
        List<Message> messages = new ArrayList<>();
        messages.add(getMessageForString("{ \"first_name\": \"john doe\"}"));
        messages.add(getMessageForString("{ \"last_name\": \"walker\"}"));

        Records actualRecords = converter.convert(messages);

        /*
        cant do assert equals because of timestamp value
        assertEquals(expectedRecords, records);
         */
        assertThat(actualRecords.getInvalidRecords(), empty());
        assertEquals(2, actualRecords.getValidRecords().size());
        Record validRecord1 = actualRecords.getValidRecords().get(0);
        assertNull(validRecord1.getErrorInfo());
        assertThat(validRecord1.getColumns(), hasEntry("first_name", "john doe"));
        Record validRecord2 = actualRecords.getValidRecords().get(1);
        assertNull(validRecord2.getErrorInfo());
        assertThat(validRecord2.getColumns(), hasEntry("last_name", "walker"));

        List<String> dateTimeList = actualRecords
                .getValidRecords()
                .stream()
                .map(k -> (String) k.getColumns().get("event_timestamp"))
                .collect(Collectors.toList());
        long currentTimeMillis = System.currentTimeMillis();
        //assert that time stamp injected is recent by checking the difference to be less than 10 seconds
        boolean timedifferenceForFirstDate = (currentTimeMillis - DF.parse(dateTimeList.get(0)).getTime()) < 60000;
        long timeDifferenceForSecondDate = currentTimeMillis - DF.parse(dateTimeList.get(1)).getTime();
        assertTrue("the difference is " + timedifferenceForFirstDate, timedifferenceForFirstDate);
        assertTrue("the difference is " + timeDifferenceForSecondDate, timeDifferenceForSecondDate < 60000);
    }

    private Message getMessageForString(String jsonStr) {
        byte[] logMessage = jsonStr.getBytes();
        return new Message(null, logMessage);
    }

    @Test
    public void shouldConvertJsonMessagesToRecordForEmpty() {
        MessageParser parser = new JsonMessageParser(defaultConfig, instrumentation, jsonParserMetrics);
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_KEY");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig);
        List<Message> messages = new ArrayList<>();
        messages.add(new Message("".getBytes(), null));
        messages.add(new Message(null, null));

        Records records = converter.convert(messages);
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, records.getInvalidRecords().get(1).getErrorInfo().getErrorType());
    }
}
