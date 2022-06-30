package io.odpf.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;
import io.odpf.depot.TestMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.config.OdpfSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ProtoOdpfMessageParserTest {

    private final HashMap<String, String> configMap = new HashMap<String, String>() {{
        put("SCHEMA_REGISTRY_STENCIL_ENABLE", "false");
        put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestMessage");
    }};

    @Test
    public void shouldParseLogMessage() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order-1").build();
        OdpfMessage message = new OdpfMessage(null, testMessage.toByteArray());
        ParsedOdpfMessage parsedOdpfMessage = protoOdpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "io.odpf.depot.TestMessage");
        assertEquals(testMessage, parsedOdpfMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidMessage() {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidMessageBytes = "invalid message".getBytes();
        OdpfMessage message = new OdpfMessage(null, invalidMessageBytes);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoOdpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "io.odpf.depot.TestMessage");
        });
    }

    @Test
    public void shouldParseLogKey() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testKey = TestMessage.newBuilder().setOrderNumber("order-1").build();
        OdpfMessage message = new OdpfMessage(testKey.toByteArray(), null);
        ParsedOdpfMessage parsedOdpfMessage = protoOdpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, "io.odpf.depot.TestMessage");
        assertEquals(testKey, parsedOdpfMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidKey() {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidKeyBytes = "invalid message".getBytes();
        OdpfMessage message = new OdpfMessage(invalidKeyBytes, null);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoOdpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_KEY, "io.odpf.depot.TestMessage");
        });
    }

    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] validKeyBytes = TestMessage.newBuilder().setOrderNumber("order-1").build().toByteArray();
        OdpfMessage message = new OdpfMessage(validKeyBytes, null);
        IOException ioException = assertThrows(IOException.class, () -> {
            protoOdpfMessageParser.parse(message, null, null);
        });
        assertEquals("parser mode not defined", ioException.getMessage());
    }
}
