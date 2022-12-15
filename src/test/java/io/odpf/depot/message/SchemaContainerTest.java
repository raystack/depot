package io.odpf.depot.message;

import com.google.protobuf.Descriptors;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestMessage;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class SchemaContainerTest {

    private final Map<String, String> configuration = new HashMap<>();
    private ProtoOdpfMessageParser odpfMessageParser;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private HttpSinkConfig sinkConfig;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestMessage");
        configuration.put("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", "io.odpf.depot.TestKey");
        sinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        HashMap<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
        }};
        odpfMessageParser = new ProtoOdpfMessageParser(stencilClient);
        when(stencilClient.getAll()).thenReturn(descriptorsMap);
    }

    @Test
    public void shouldReturnSchemaKey() throws IOException {
        String schemaProtoKeyClass = sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        OdpfMessageSchema expectedSchema = odpfMessageParser.getSchema(schemaProtoKeyClass);
        SchemaContainer schemaContainer = new SchemaContainer(odpfMessageParser);
        OdpfMessageSchema schema = schemaContainer.getSchemaKey(schemaProtoKeyClass);
        Assert.assertEquals(expectedSchema, schema);
    }

    @Test
    public void shouldReturnSchemaMessage() throws IOException {
        String schemaProtoMessageClassClass = sinkConfig.getSinkConnectorSchemaProtoMessageClass();
        OdpfMessageSchema expectedSchema = odpfMessageParser.getSchema(schemaProtoMessageClassClass);
        SchemaContainer schemaContainer = new SchemaContainer(odpfMessageParser);
        OdpfMessageSchema schema = schemaContainer.getSchemaMessage(schemaProtoMessageClassClass);
        Assert.assertEquals(expectedSchema, schema);
    }
}
