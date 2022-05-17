package io.odpf.depot.bigquery.handler;

import com.google.common.collect.ImmutableMap;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.bigquery.models.Records;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.json.JsonOdpfMessageParser;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MessageRecordConverterForJsonTest {

    private final OdpfSinkConfig defaultConfig = ConfigFactory.create(OdpfSinkConfig.class, Collections.emptyMap());
    private final Record.RecordBuilder recordBuilder = Record.builder();
    private final Map<String, Object> emptyMetadata = Collections.emptyMap();
    private final Map<String, Object> emptyColumnsMap = Collections.emptyMap();
    private final ErrorInfo noError = null;

    @Test
    public void shouldReturnEmptyRecordsforEmptyList() {
        OdpfMessageParser parser = new JsonOdpfMessageParser(defaultConfig);
        OdpfMessageSchema schema = null;
        BigQuerySinkConfig bigQuerySinkConfig = null;
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig, schema);
        List<OdpfMessage> emptyOdpfMessageList = Collections.emptyList();

        Records records = converter.convert(emptyOdpfMessageList);
        List<Record> emptyRecordList = Collections.emptyList();
        Records expectedRecords = new Records(emptyRecordList, emptyRecordList);
        assertEquals(expectedRecords, records);
    }

    @Test
    public void shouldConvertJsonMessagesToRecordForLogMessage() {
        OdpfMessageParser parser = new JsonOdpfMessageParser(defaultConfig);
        OdpfMessageSchema schema = null;
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_MESSAGE");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig, schema);
        List<OdpfMessage> messages = new ArrayList<>();
        messages.add(getOdpfMessageForString("{ \"first_name\": \"john doe\"}"));
        messages.add(getOdpfMessageForString("{ \"last_name\": \"walker\"}"));

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
        OdpfMessageParser parser = new JsonOdpfMessageParser(defaultConfig);
        OdpfMessageSchema schema = null;
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_KEY");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig, schema);
        List<OdpfMessage> messages = new ArrayList<>();
        messages.add(new OdpfMessage("{ \"first_name\": \"john doe\"}".getBytes(), null));
        messages.add(new OdpfMessage("{ \"last_name\": \"walker\"}".getBytes(), null));

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
        OdpfMessageParser parser = new JsonOdpfMessageParser(defaultConfig);
        OdpfMessageSchema schema = null;
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "LOG_MESSAGE");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configMap);
        MessageRecordConverter converter = new MessageRecordConverter(parser, bigQuerySinkConfig, schema);
        List<OdpfMessage> messages = new ArrayList<>();
        messages.add(getOdpfMessageForString("{ \"first_name\": \"john doe\"}"));
        messages.add(getOdpfMessageForString("{ invalid json str"));
        messages.add(getOdpfMessageForString("{ \"last_name\": \"walker\"}"));
        messages.add(getOdpfMessageForString("another invalid message"));
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

        messages.add(getOdpfMessageForString(nestedJsonStr));

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

    private OdpfMessage getOdpfMessageForString(String jsonStr) {
        byte[] logMessage = jsonStr.getBytes();
        OdpfMessage odpfMessage = new OdpfMessage(null, logMessage);
        return odpfMessage;
    }
}
