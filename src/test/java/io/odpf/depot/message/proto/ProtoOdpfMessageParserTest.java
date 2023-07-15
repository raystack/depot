package org.raystack.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import org.raystack.depot.message.ParsedOdpfMessage;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.OdpfStencilUpdateListener;
import org.raystack.depot.TestMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.OdpfMessage;
import org.raystack.depot.config.OdpfSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ProtoOdpfMessageParserTest {

    private final HashMap<String, String> configMap = new HashMap<String, String>() {
        {
            put("SCHEMA_REGISTRY_STENCIL_ENABLE", "false");
            put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "org.raystack.depot.TestMessage");
        }
    };

    @Test
    public void shouldParseLogMessage() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter,
                protoUpdateListener);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order-1").build();
        OdpfMessage message = new OdpfMessage(null, testMessage.toByteArray());
        ParsedOdpfMessage parsedOdpfMessage = protoOdpfMessageParser.parse(message,
                SinkConnectorSchemaMessageMode.LOG_MESSAGE, "org.raystack.depot.TestMessage");
        assertEquals(testMessage, parsedOdpfMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidMessage() {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter,
                protoUpdateListener);
        byte[] invalidMessageBytes = "invalid message".getBytes();
        OdpfMessage message = new OdpfMessage(null, invalidMessageBytes);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoOdpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE,
                    "org.raystack.depot.TestMessage");
        });
    }

    @Test
    public void shouldParseLogKey() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter,
                protoUpdateListener);
        TestMessage testKey = TestMessage.newBuilder().setOrderNumber("order-1").build();
        OdpfMessage message = new OdpfMessage(testKey.toByteArray(), null);
        ParsedOdpfMessage parsedOdpfMessage = protoOdpfMessageParser.parse(message,
                SinkConnectorSchemaMessageMode.LOG_KEY, "org.raystack.depot.TestMessage");
        assertEquals(testKey, parsedOdpfMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidKey() {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter,
                protoUpdateListener);
        byte[] invalidKeyBytes = "invalid message".getBytes();
        OdpfMessage message = new OdpfMessage(invalidKeyBytes, null);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoOdpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY,
                    "org.raystack.depot.TestMessage");
        });
    }

    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter,
                protoUpdateListener);
        byte[] validKeyBytes = TestMessage.newBuilder().setOrderNumber("order-1").build().toByteArray();
        OdpfMessage message = new OdpfMessage(validKeyBytes, null);
        IOException ioException = assertThrows(IOException.class, () -> {
            protoOdpfMessageParser.parse(message, null, null);
        });
        assertEquals("parser mode not defined", ioException.getMessage());
    }
}
