package io.odpf.depot.redis.parsers;

import com.google.protobuf.Descriptors;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestMessage;
import io.odpf.depot.TestNestedMessage;
import io.odpf.depot.TestNestedRepeatedMessage;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.*;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.message.proto.ProtoOdpfParsedMessage;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisKeyValueEntry;
import io.odpf.depot.redis.enums.RedisSinkDataType;
import io.odpf.depot.redis.record.RedisRecord;
import io.odpf.depot.utils.MessageConfigUtils;
import io.odpf.stencil.Parser;
import io.odpf.stencil.StencilClientFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisParserTest {
    private final List<OdpfMessage> messages = new ArrayList<>();
    private final String schemaClass = "io.odpf.depot.TestMessage";
    private final Configuration configuration = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();
    private final JsonFormat.Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames()
            .includingDefaultValueFields();
    private final Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
        put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
        put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
        put(String.format("%s", TestNestedMessage.class.getName()), TestNestedMessage.getDescriptor());
        put(String.format("%s", TestNestedRepeatedMessage.class.getName()), TestNestedRepeatedMessage.getDescriptor());
    }};
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private ProtoOdpfMessageParser odpfMessageParser;
    @Mock
    private StatsDReporter statsDReporter;
    private RedisParser redisParser;

    @Before
    public void setup() throws IOException {
        when(redisSinkConfig.getSinkRedisDataType()).thenReturn(RedisSinkDataType.KEYVALUE);
        when(redisSinkConfig.getSinkRedisKeyTemplate()).thenReturn("test-key");
        when(redisSinkConfig.getSinkRedisKeyValueDataFieldName()).thenReturn("order_number");
        when(redisSinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        when(redisSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn(schemaClass);
        when(redisSinkConfig.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        TestMessage message1 = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        TestMessage message2 = TestMessage.newBuilder().setOrderNumber("test-order-2").setOrderDetails("ORDER-DETAILS-2").build();
        TestMessage message3 = TestMessage.newBuilder().setOrderNumber("test-order-3").setOrderDetails("ORDER-DETAILS-3").build();
        TestMessage message4 = TestMessage.newBuilder().setOrderNumber("test-order-4").setOrderDetails("ORDER-DETAILS-4").build();
        TestMessage message5 = TestMessage.newBuilder().setOrderNumber("test-order-5").setOrderDetails("ORDER-DETAILS-5").build();
        TestMessage message6 = TestMessage.newBuilder().setOrderNumber("test-order-6").setOrderDetails("ORDER-DETAILS-6").build();
        messages.add(new OdpfMessage(null, message1.toByteArray()));
        messages.add(new OdpfMessage(null, message2.toByteArray()));
        messages.add(new OdpfMessage(null, message3.toByteArray()));
        messages.add(new OdpfMessage(null, message4.toByteArray()));
        messages.add(new OdpfMessage(null, message5.toByteArray()));
        messages.add(new OdpfMessage(null, message6.toByteArray()));
    }

    public void setupParserResponse() throws IOException {
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessage.class.getName());
        for (OdpfMessage message : messages) {
            ParsedOdpfMessage parsedOdpfMessage = new ProtoOdpfParsedMessage(protoParser.parse((byte[]) message.getLogMessage()), configuration, jsonPrinter);
            when(odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenReturn(parsedOdpfMessage);
        }
        ProtoOdpfMessageParser messageParser = (ProtoOdpfMessageParser) OdpfMessageParserFactory.getParser(redisSinkConfig, statsDReporter);
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(redisSinkConfig);
        OdpfMessageSchema schema = messageParser.getSchema(modeAndSchema.getSecond(), descriptorsMap);
        RedisEntryParser redisEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter, schema);
        redisParser = new RedisParser(odpfMessageParser, redisEntryParser, modeAndSchema);
    }

    @Test
    public void shouldConvertOdpfMessageToRedisRecords() throws IOException {
        setupParserResponse();
        List<RedisRecord> parsedRecords = redisParser.convert(messages);
        Map<Boolean, List<RedisRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(RedisRecord::isValid));
        List<RedisRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<RedisRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(6, validRecords.size());
        assertTrue(invalidRecords.isEmpty());
        List<RedisRecord> expectedRecords = new ArrayList<>();
        expectedRecords.add(new RedisRecord(new RedisKeyValueEntry("test-key", "test-order-1", null), 0L, null, "{}", true));
        expectedRecords.add(new RedisRecord(new RedisKeyValueEntry("test-key", "test-order-2", null), 1L, null, "{}", true));
        expectedRecords.add(new RedisRecord(new RedisKeyValueEntry("test-key", "test-order-3", null), 2L, null, "{}", true));
        expectedRecords.add(new RedisRecord(new RedisKeyValueEntry("test-key", "test-order-4", null), 3L, null, "{}", true));
        expectedRecords.add(new RedisRecord(new RedisKeyValueEntry("test-key", "test-order-5", null), 4L, null, "{}", true));
        expectedRecords.add(new RedisRecord(new RedisKeyValueEntry("test-key", "test-order-6", null), 5L, null, "{}", true));
        IntStream.range(0, expectedRecords.size()).forEach(index -> assertEquals(expectedRecords.get(index).toString(), parsedRecords.get(index).toString()));
    }

    @Test
    public void shouldReportValidAndInvalidRecords() throws IOException {
        setupParserResponse();
        when(odpfMessageParser.parse(messages.get(2), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new IOException("Error while parsing protobuf"));
        when(odpfMessageParser.parse(messages.get(3), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new ConfigurationException("Invalid field config : INVALID"));
        when(odpfMessageParser.parse(messages.get(4), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new IllegalArgumentException("Config REDIS_CONFIG is empty"));
        when(odpfMessageParser.parse(messages.get(5), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new UnsupportedOperationException("some message"));
        List<RedisRecord> parsedRecords = redisParser.convert(messages);
        Map<Boolean, List<RedisRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(RedisRecord::isValid));
        List<RedisRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<RedisRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, validRecords.size());
        assertEquals(4, invalidRecords.size());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, parsedRecords.get(2).getErrorInfo().getErrorType());
        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, parsedRecords.get(3).getErrorInfo().getErrorType());
        assertEquals(ErrorType.DEFAULT_ERROR, parsedRecords.get(4).getErrorInfo().getErrorType());
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, parsedRecords.get(5).getErrorInfo().getErrorType());
    }
}
