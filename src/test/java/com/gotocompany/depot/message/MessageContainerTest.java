package com.gotocompany.depot.message;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestServiceType;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageContainerTest {
    private final Map<String, String> configuration = new HashMap<>();
    private ProtoMessageParser messageParser;
    private Message message;
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private HttpSinkConfig sinkConfig;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "com.gotocompany.depot.TestBookingLogKey");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        messageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);

        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        message = new Message(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
    }

    @Test
    public void shouldReturnParsedLogKey() throws IOException {
        ParsedMessage expectedParsedLogKey = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());
        MessageContainer messageContainer = new MessageContainer(message, messageParser);
        ParsedMessage parsedLogKey = messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass());
        Assert.assertEquals(expectedParsedLogKey.getRaw(), parsedLogKey.getRaw());
    }

    @Test
    public void shouldReturnParsedLogMessage() throws IOException {
        ParsedMessage expectedParsedLogMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        MessageContainer messageContainer = new MessageContainer(message, messageParser);
        ParsedMessage parsedLogMessage = messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        Assert.assertEquals(expectedParsedLogMessage.getRaw(), parsedLogMessage.getRaw());
    }
}
