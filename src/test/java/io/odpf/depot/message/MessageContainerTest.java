package io.odpf.depot.message;

import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
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
    private ProtoOdpfMessageParser odpfMessageParser;
    private OdpfMessage message;
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private HttpSinkConfig sinkConfig;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestBookingLogMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "io.odpf.depot.TestBookingLogKey");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        odpfMessageParser = (ProtoOdpfMessageParser) OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);

        TestBookingLogKey bookingLogKey = TestBookingLogKey.newBuilder().setOrderNumber("ON#1").setOrderUrl("OURL#1").build();
        TestBookingLogMessage bookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("ON#1").setServiceType(TestServiceType.Enum.GO_SEND).setCancelReasonId(1).build();
        message = new OdpfMessage(bookingLogKey.toByteArray(), bookingLogMessage.toByteArray());
    }

    @Test
    public void shouldReturnParsedLogKey() throws IOException {
        ParsedOdpfMessage expectedParsedLogKey = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, sinkConfig.getSinkConnectorSchemaProtoKeyClass());
        MessageContainer messageContainer = new MessageContainer(message, odpfMessageParser);
        ParsedOdpfMessage parsedLogKey = messageContainer.getParsedLogKey(sinkConfig.getSinkConnectorSchemaProtoKeyClass());
        Assert.assertEquals(expectedParsedLogKey.getRaw(), parsedLogKey.getRaw());
    }

    @Test
    public void shouldReturnParsedLogMessage() throws IOException {
        ParsedOdpfMessage expectedParsedLogMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        MessageContainer messageContainer = new MessageContainer(message, odpfMessageParser);
        ParsedOdpfMessage parsedLogMessage = messageContainer.getParsedLogMessage(sinkConfig.getSinkConnectorSchemaProtoMessageClass());
        Assert.assertEquals(expectedParsedLogMessage.getRaw(), parsedLogMessage.getRaw());
    }
}
