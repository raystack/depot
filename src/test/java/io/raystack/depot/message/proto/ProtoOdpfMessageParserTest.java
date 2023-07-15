package org.raystack.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.RaystackStencilUpdateListener;
import org.raystack.depot.TestMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.config.RaystackSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ProtoRaystackMessageParserTest {

    private final HashMap<String, String> configMap = new HashMap<String, String>() {
        {
            put("SCHEMA_REGISTRY_STENCIL_ENABLE", "false");
            put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "org.raystack.depot.TestMessage");
        }
    };

    @Test
    public void shouldParseLogMessage() throws IOException {
        RaystackSinkConfig sinkConfig = ConfigFactory.create(RaystackSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        RaystackStencilUpdateListener protoUpdateListener = mock(RaystackStencilUpdateListener.class);
        ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                statsdReporter,
                protoUpdateListener);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order-1").build();
        RaystackMessage message = new RaystackMessage(null, testMessage.toByteArray());
        ParsedRaystackMessage parsedRaystackMessage = protoRaystackMessageParser.parse(message,
                SinkConnectorSchemaMessageMode.LOG_MESSAGE, "org.raystack.depot.TestMessage");
        assertEquals(testMessage, parsedRaystackMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidMessage() {
        RaystackSinkConfig sinkConfig = ConfigFactory.create(RaystackSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        RaystackStencilUpdateListener protoUpdateListener = mock(RaystackStencilUpdateListener.class);
        ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                statsdReporter,
                protoUpdateListener);
        byte[] invalidMessageBytes = "invalid message".getBytes();
        RaystackMessage message = new RaystackMessage(null, invalidMessageBytes);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoRaystackMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE,
                    "org.raystack.depot.TestMessage");
        });
    }

    @Test
    public void shouldParseLogKey() throws IOException {
        RaystackSinkConfig sinkConfig = ConfigFactory.create(RaystackSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        RaystackStencilUpdateListener protoUpdateListener = mock(RaystackStencilUpdateListener.class);
        ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                statsdReporter,
                protoUpdateListener);
        TestMessage testKey = TestMessage.newBuilder().setOrderNumber("order-1").build();
        RaystackMessage message = new RaystackMessage(testKey.toByteArray(), null);
        ParsedRaystackMessage parsedRaystackMessage = protoRaystackMessageParser.parse(message,
                SinkConnectorSchemaMessageMode.LOG_KEY, "org.raystack.depot.TestMessage");
        assertEquals(testKey, parsedRaystackMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidKey() {
        RaystackSinkConfig sinkConfig = ConfigFactory.create(RaystackSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        RaystackStencilUpdateListener protoUpdateListener = mock(RaystackStencilUpdateListener.class);
        ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                statsdReporter,
                protoUpdateListener);
        byte[] invalidKeyBytes = "invalid message".getBytes();
        RaystackMessage message = new RaystackMessage(invalidKeyBytes, null);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoRaystackMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY,
                    "org.raystack.depot.TestMessage");
        });
    }

    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        RaystackSinkConfig sinkConfig = ConfigFactory.create(RaystackSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        RaystackStencilUpdateListener protoUpdateListener = mock(RaystackStencilUpdateListener.class);
        ProtoRaystackMessageParser protoRaystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                statsdReporter,
                protoUpdateListener);
        byte[] validKeyBytes = TestMessage.newBuilder().setOrderNumber("order-1").build().toByteArray();
        RaystackMessage message = new RaystackMessage(validKeyBytes, null);
        IOException ioException = assertThrows(IOException.class, () -> {
            protoRaystackMessageParser.parse(message, null, null);
        });
        assertEquals("parser mode not defined", ioException.getMessage());
    }
}
