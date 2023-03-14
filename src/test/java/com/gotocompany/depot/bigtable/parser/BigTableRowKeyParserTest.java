package com.gotocompany.depot.bigtable.parser;

import com.google.protobuf.Descriptors;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestNestedMessage;
import com.gotocompany.depot.TestNestedRepeatedMessage;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.exception.InvalidTemplateException;
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
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoMessageParser messageParser = new ProtoMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
        MessageSchema schema = messageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);

        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("eureka")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage);
        ParsedMessage parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());

        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);
        String parsedRowKey = bigTableRowKeyParser.parse(parsedMessage);
        assertEquals("row-xyz-order$key#eureka*test", parsedRowKey);
    }

    @Test
    public void shouldReturnTheRowKeySameAsTemplateWhenTemplateIsValidAndContainsOnlyConstantStrings() throws IOException, InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key#constant$String");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoMessageParser messageParser = new ProtoMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
        MessageSchema schema = messageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);

        byte[] logMessage = TestMessage.newBuilder()
                .setOrderNumber("xyz-order")
                .setOrderDetails("eureka")
                .build()
                .toByteArray();
        Message message = new Message(null, logMessage);
        ParsedMessage parsedMessage = messageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, sinkConfig.getSinkConnectorSchemaProtoMessageClass());

        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema);
        String parsedRowKey = bigTableRowKeyParser.parse(parsedMessage);
        assertEquals("row-key#constant$String", parsedRowKey);
    }

    @Test
    public void shouldThrowErrorForInvalidTemplate() throws IOException {
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key%s");
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

        ProtoMessageParser messageParser = new ProtoMessageParser(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
        MessageSchema schema = messageParser.getSchema(sinkConfig.getSinkConnectorSchemaProtoMessageClass(), descriptorsMap);

        InvalidTemplateException illegalArgumentException = Assertions.assertThrows(InvalidTemplateException.class, () -> new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema));
        assertEquals("Template is not valid, variables=1, validArgs=1, values=0", illegalArgumentException.getMessage());
    }

}
