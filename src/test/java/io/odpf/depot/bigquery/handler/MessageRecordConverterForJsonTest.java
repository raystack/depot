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

    @Test
    public void shouldReturnEmptyRecordsforEmptyList() {
        OdpfSinkConfig odpfSinkConfig = null;
        OdpfMessageParser parser = new JsonOdpfMessageParser(odpfSinkConfig);
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
    public void shouldConvertJsonMessagesToRecord() {
        OdpfSinkConfig odpfSinkConfig = null;
        OdpfMessageParser parser = new JsonOdpfMessageParser(odpfSinkConfig);
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

        Map<String, Object> metadata = Collections.emptyMap();

        long index = 0L;
        ErrorInfo errorInfo = null;
        expectedValidRecords.add(new Record(metadata, ImmutableMap.of("first_name", "john doe"), 0L, errorInfo));
        expectedValidRecords.add(new Record(metadata, ImmutableMap.of("last_name", "walker"), 1L, errorInfo));
        List<Record> invalidRecords = Collections.emptyList();
        Records expectedRecords = new Records(expectedValidRecords, invalidRecords);
        assertEquals(expectedRecords, records);


    }

    @Test
    public void shouldHandleBothInvalidAndValidJsonMessages() {
        OdpfSinkConfig odpfSinkConfig = null;
        OdpfMessageParser parser = new JsonOdpfMessageParser(odpfSinkConfig);
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
        Records records = converter.convert(messages);


        Map<String, Object> emptyMetadata = Collections.emptyMap();

        ErrorInfo nullErrorInfo = null;
        List<Record> expectedValidRecords = new ArrayList<>();
        expectedValidRecords.add(new Record(emptyMetadata, ImmutableMap.of("first_name", "john doe"), 0L, nullErrorInfo));
        expectedValidRecords.add(new Record(emptyMetadata, ImmutableMap.of("last_name", "walker"), 2L, nullErrorInfo));

        ErrorInfo errorInfo = new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR);
        List<Record> invalidRecords = new ArrayList<>();
        invalidRecords.add(new Record(emptyMetadata, Collections.emptyMap(), 1L, errorInfo));
        invalidRecords.add(new Record(emptyMetadata, Collections.emptyMap(), 3L, errorInfo));
        assertEquals(expectedValidRecords, records.getValidRecords());

    }

    private OdpfMessage getOdpfMessageForString(String jsonStr) {
        byte[] logMessage = jsonStr.getBytes();
        OdpfMessage odpfMessage = new OdpfMessage(null, logMessage);
        return odpfMessage;
    }
}
