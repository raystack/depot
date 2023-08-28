package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.message.*;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.redis.client.entry.RedisKeyValueEntry;
import com.gotocompany.stencil.Parser;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.utils.MessageConfigUtils;
import com.jayway.jsonpath.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisParserTest {
    private final List<Message> messages = new ArrayList<>();
    private final String schemaClass = "com.gotocompany.depot.TestMessage";
    @Mock
    private RedisSinkConfig redisSinkConfig;
    @Mock
    private ProtoMessageParser protoMessageParser;
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
        when(redisSinkConfig.getSinkDefaultFieldValueEnable()).thenReturn(true);
        TestMessage message1 = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        TestMessage message2 = TestMessage.newBuilder().setOrderNumber("test-order-2").setOrderDetails("ORDER-DETAILS-2").build();
        TestMessage message3 = TestMessage.newBuilder().setOrderNumber("test-order-3").setOrderDetails("ORDER-DETAILS-3").build();
        TestMessage message4 = TestMessage.newBuilder().setOrderNumber("test-order-4").setOrderDetails("ORDER-DETAILS-4").build();
        TestMessage message5 = TestMessage.newBuilder().setOrderNumber("test-order-5").setOrderDetails("ORDER-DETAILS-5").build();
        TestMessage message6 = TestMessage.newBuilder().setOrderNumber("test-order-6").setOrderDetails("ORDER-DETAILS-6").build();
        messages.add(new Message(null, message1.toByteArray()));
        messages.add(new Message(null, message2.toByteArray()));
        messages.add(new Message(null, message3.toByteArray()));
        messages.add(new Message(null, message4.toByteArray()));
        messages.add(new Message(null, message5.toByteArray()));
        messages.add(new Message(null, message6.toByteArray()));
    }

    public void setupParserResponse() throws IOException {
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessage.class.getName());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(redisSinkConfig))
                .build();
        for (Message message : messages) {
            ParsedMessage parsedMessage = new ProtoParsedMessage(protoParser.parse((byte[]) message.getLogMessage()), jsonPathConfig);
            when(protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenReturn(parsedMessage);
        }
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(redisSinkConfig);
        RedisEntryParser redisEntryParser = RedisEntryParserFactory.getRedisEntryParser(redisSinkConfig, statsDReporter);
        redisParser = new RedisParser(this.protoMessageParser, redisEntryParser, modeAndSchema);
    }

    @Test
    public void shouldConvertMessageToRedisRecords() throws IOException {
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
        when(protoMessageParser.parse(messages.get(2), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new IOException("Error while parsing protobuf"));
        when(protoMessageParser.parse(messages.get(3), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new ConfigurationException("Invalid field config : INVALID"));
        when(protoMessageParser.parse(messages.get(4), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new IllegalArgumentException("Config REDIS_CONFIG is empty"));
        when(protoMessageParser.parse(messages.get(5), SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass)).thenThrow(new UnsupportedOperationException("some message"));
        List<RedisRecord> parsedRecords = redisParser.convert(messages);
        Map<Boolean, List<RedisRecord>> splitterRecords = parsedRecords.stream().collect(Collectors.partitioningBy(RedisRecord::isValid));
        List<RedisRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<RedisRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        assertEquals(2, validRecords.size());
        assertEquals(4, invalidRecords.size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, parsedRecords.get(2).getErrorInfo().getErrorType());
        Assert.assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, parsedRecords.get(3).getErrorInfo().getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, parsedRecords.get(4).getErrorInfo().getErrorType());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, parsedRecords.get(5).getErrorInfo().getErrorType());
    }
}
