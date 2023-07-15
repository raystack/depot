package org.raystack.depot.bigtable.parser;

import com.google.protobuf.Descriptors;
import com.timgroup.statsd.NoOpStatsDClient;
import org.raystack.depot.TestKey;
import org.raystack.depot.TestMessage;
import org.raystack.depot.TestNestedMessage;
import org.raystack.depot.TestNestedRepeatedMessage;
import org.raystack.depot.common.Template;
import org.raystack.depot.config.BigTableSinkConfig;
import org.raystack.depot.exception.InvalidTemplateException;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.proto.ProtoRaystackMessageParser;
import org.raystack.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BigTableRowKeyParserTest {

        private final Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {
                {
                        put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
                        put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
                        put(String.format("%s", TestNestedMessage.class.getName()), TestNestedMessage.getDescriptor());
                        put(String.format("%s", TestNestedRepeatedMessage.class.getName()),
                                        TestNestedRepeatedMessage.getDescriptor());
                }
        };

        @Test
        public void shouldReturnParsedRowKeyForValidParameterisedTemplate()
                        throws IOException, InvalidTemplateException {
                System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-%s$key#%s*test,order_number,order_details");
                System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "org.raystack.depot.TestMessage");
                BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

                ProtoRaystackMessageParser raystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                                new StatsDReporter(new NoOpStatsDClient()), null);
                RaystackMessageSchema schema = raystackMessageParser.getSchema(
                                sinkConfig.getSinkConnectorSchemaProtoMessageClass(),
                                descriptorsMap);

                byte[] logMessage = TestMessage.newBuilder()
                                .setOrderNumber("xyz-order")
                                .setOrderDetails("eureka")
                                .build()
                                .toByteArray();
                RaystackMessage message = new RaystackMessage(null, logMessage);
                ParsedRaystackMessage parsedRaystackMessage = raystackMessageParser.parse(message,
                                SinkConnectorSchemaMessageMode.LOG_MESSAGE,
                                sinkConfig.getSinkConnectorSchemaProtoMessageClass());

                BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(
                                new Template(sinkConfig.getRowKeyTemplate()), schema);
                String parsedRowKey = bigTableRowKeyParser.parse(parsedRaystackMessage);
                assertEquals("row-xyz-order$key#eureka*test", parsedRowKey);
        }

        @Test
        public void shouldReturnTheRowKeySameAsTemplateWhenTemplateIsValidAndContainsOnlyConstantStrings()
                        throws IOException, InvalidTemplateException {
                System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key#constant$String");
                System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "org.raystack.depot.TestMessage");
                BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

                ProtoRaystackMessageParser raystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                                new StatsDReporter(new NoOpStatsDClient()), null);
                RaystackMessageSchema schema = raystackMessageParser.getSchema(
                                sinkConfig.getSinkConnectorSchemaProtoMessageClass(),
                                descriptorsMap);

                byte[] logMessage = TestMessage.newBuilder()
                                .setOrderNumber("xyz-order")
                                .setOrderDetails("eureka")
                                .build()
                                .toByteArray();
                RaystackMessage message = new RaystackMessage(null, logMessage);
                ParsedRaystackMessage parsedRaystackMessage = raystackMessageParser.parse(message,
                                SinkConnectorSchemaMessageMode.LOG_MESSAGE,
                                sinkConfig.getSinkConnectorSchemaProtoMessageClass());

                BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(
                                new Template(sinkConfig.getRowKeyTemplate()), schema);
                String parsedRowKey = bigTableRowKeyParser.parse(parsedRaystackMessage);
                assertEquals("row-key#constant$String", parsedRowKey);
        }

        @Test
        public void shouldThrowErrorForInvalidTemplate() throws IOException {
                System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key%s");
                System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "org.raystack.depot.TestMessage");
                BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());

                ProtoRaystackMessageParser raystackMessageParser = new ProtoRaystackMessageParser(sinkConfig,
                                new StatsDReporter(new NoOpStatsDClient()), null);
                RaystackMessageSchema schema = raystackMessageParser.getSchema(
                                sinkConfig.getSinkConnectorSchemaProtoMessageClass(),
                                descriptorsMap);

                InvalidTemplateException illegalArgumentException = Assertions.assertThrows(
                                InvalidTemplateException.class,
                                () -> new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()), schema));
                assertEquals("Template is not valid, variables=1, validArgs=1, values=0",
                                illegalArgumentException.getMessage());
        }

}
