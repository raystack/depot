package io.odpf.sink.connectors.message;

import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.sink.connectors.TestMessage;
import io.odpf.sink.connectors.stencil.OdpfStencilUpdateListener;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.message.proto.ProtoOdpfMessage;
import io.odpf.sink.connectors.message.proto.ProtoOdpfMessageParser;
import io.odpf.sink.connectors.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ProtoOdpfMessageParserTest {

    private final HashMap<String, String> configMap = new HashMap<String, String>() {{
        put("SCHEMA_REGISTRY_STENCIL_ENABLE", "false");
        put("SINK_CONNECTOR_SCHEMA_MESSAGE_CLASS", "io.odpf.sink.connectors.TestMessage");
    }};

    @Test
    public void shouldParseLogMessage() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order-1").build();
        OdpfMessage message = new ProtoOdpfMessage(null, testMessage.toByteArray());
        ParsedOdpfMessage parsedOdpfMessage = protoOdpfMessageParser.parse(message, InputSchemaMessageMode.LOG_MESSAGE, "io.odpf.sink.connectors.TestMessage");
        assertEquals(testMessage, parsedOdpfMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidMessage() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidMessageBytes = "invalid message".getBytes();
        OdpfMessage message = new ProtoOdpfMessage(null, invalidMessageBytes);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoOdpfMessageParser.parse(message, InputSchemaMessageMode.LOG_MESSAGE, "io.odpf.sink.connectors.TestMessage");
        });


    }

    @Test
    public void shouldParseLogKey() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        TestMessage testKey = TestMessage.newBuilder().setOrderNumber("order-1").build();
        OdpfMessage message = new ProtoOdpfMessage(testKey.toByteArray(), null);
        ParsedOdpfMessage parsedOdpfMessage = protoOdpfMessageParser.parse(message, InputSchemaMessageMode.LOG_KEY, "io.odpf.sink.connectors.TestMessage");
        assertEquals(testKey, parsedOdpfMessage.getRaw());

    }

    @Test
    public void shouldThrowErrorOnInvalidKey() throws IOException {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] invalidKeyBytes = "invalid message".getBytes();
        OdpfMessage message = new ProtoOdpfMessage(invalidKeyBytes, null);
        assertThrows(InvalidProtocolBufferException.class, () -> {
            protoOdpfMessageParser.parse(message, InputSchemaMessageMode.LOG_KEY, "io.odpf.sink.connectors.TestMessage");
        });
    }

    @Test
    public void shouldThrowErrorWhenModeNotDefined() {
        OdpfSinkConfig sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        StatsDReporter statsdReporter = mock(StatsDReporter.class);
        OdpfStencilUpdateListener protoUpdateListener = mock(OdpfStencilUpdateListener.class);
        ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, statsdReporter, protoUpdateListener);
        byte[] validKeyBytes = TestMessage.newBuilder().setOrderNumber("order-1").build().toByteArray();
        OdpfMessage message = new ProtoOdpfMessage(validKeyBytes, null);
        IOException ioException = assertThrows(IOException.class, () -> {
            protoOdpfMessageParser.parse(message, null, null);
        });
        assertEquals("parser mode not defined", ioException.getMessage());
    }
}
