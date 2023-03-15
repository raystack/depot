package com.gotocompany.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.depot.message.Message;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ProtoMessageParserTest {

    private final HashMap<String, String> configMap = new HashMap<String, String>() {{
        put("SCHEMA_REGISTRY_STENCIL_ENABLE", "false");
        put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
    }};

    @Test
    public void shouldParseLogMessage() throws IOException {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order-1").build();
        Message message = new Message(null, testMessage.toByteArray());
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage");
        assertEquals(testMessage, parsedMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidMessage() {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidMessageBytes = "invalid message".getBytes();
        Message message = new Message(null, invalidMessageBytes);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage");
        });
    }

    @Test
    public void shouldParseLogKey() throws IOException {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testKey = TestMessage.newBuilder().setOrderNumber("order-1").build();
        Message message = new Message(testKey.toByteArray(), null);
        ParsedMessage parsedMessage = protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, "com.gotocompany.depot.TestMessage");
        assertEquals(testKey, parsedMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidKey() {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidKeyBytes = "invalid message".getBytes();
        Message message = new Message(invalidKeyBytes, null);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, "com.gotocompany.depot.TestMessage");
        });
    }

    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        DepotStencilUpdateListener protoUpdateListener = mock(DepotStencilUpdateListener.class);
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] validKeyBytes = TestMessage.newBuilder().setOrderNumber("order-1").build().toByteArray();
        Message message = new Message(validKeyBytes, null);
        IOException ioException = assertThrows(IOException.class, () -> {
            protoMessageParser.parse(message, null, null);
        });
        assertEquals("parser mode not defined", ioException.getMessage());
    }
}
