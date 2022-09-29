package io.odpf.depot.bigtable.parser;

import com.google.protobuf.Descriptors;
import com.timgroup.statsd.NoOpStatsDClient;
import io.odpf.depot.TestKey;
import io.odpf.depot.TestMessage;
import io.odpf.depot.TestNestedMessage;
import io.odpf.depot.TestNestedRepeatedMessage;
import io.odpf.depot.common.Template;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BigTableRowKeyParserTest {

    private final Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
        put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
        put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
        put(String.format("%s", TestNestedMessage.class.getName()), TestNestedMessage.getDescriptor());
        put(String.format("%s", TestNestedRepeatedMessage.class.getName()), TestNestedRepeatedMessage.getDescriptor());
    }};

    @Test
    public void shouldReturnParsedRowKeyForValidParameterisedTemplate() throws IOException, InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-%s$key#%s*test,order_number,order_details");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);

        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("eureka")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());

        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);
        String parsedRowKey = bigTableRowKeyParser.parse(parsedOdpfMessage);
        assertEquals("row-xyz-order$key#eureka*test", parsedRowKey);
    }

    @Test
    public void shouldReturnTheRowKeySameAsTemplateWhenTemplateIsValidAndContainsOnlyConstantStrings() throws IOException, InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key#constant$String");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);

        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("eureka")
                .build()
                .toByteArray();
        OdpfMessage message = new OdpfMessage(null, logMessage);
        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());

        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);
        String parsedRowKey = bigTableRowKeyParser.parse(parsedOdpfMessage);
        assertEquals("row-key#constant$String", parsedRowKey);
    }

    @Test
    public void shouldThrowErrorForInvalidTemplate() throws IOException {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key%s");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "io.odpf.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoOdpfMessageParser odpfMessageParser = new ProtoOdpfMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
        OdpfMessageSchema schema = odpfMessageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);

        InvalidTemplateException illegalArgumentException = Assertions.assertThrows(InvalidTemplateException.class, () -> new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema));
        assertEquals("Template is not valid, variables=1, validArgs=1, values=0", illegalArgumentException.getMessage());
    }

}
