package org.raystack.depot.common;

import com.google.protobuf.Descriptors;
import org.raystack.depot.TestBookingLogMessage;
import org.raystack.depot.TestKey;
import org.raystack.depot.TestLocation;
import org.raystack.depot.TestMessage;
import org.raystack.depot.config.SinkConfig;
import org.raystack.depot.config.enums.SinkConnectorSchemaDataType;
import org.raystack.depot.exception.InvalidTemplateException;
import org.raystack.depot.message.Message;
import org.raystack.depot.message.MessageParserFactory;
import org.raystack.depot.message.MessageSchema;
import org.raystack.depot.message.ParsedMessage;
import org.raystack.depot.message.proto.ProtoMessageParser;
import org.raystack.depot.message.proto.ProtoParsedMessage;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.stencil.Parser;
import org.raystack.stencil.StencilClientFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TemplateTest {
    @Mock
    private SinkConfig sinkConfig;
    @Mock
    private StatsDReporter statsDReporter;
    private ParsedMessage parsedTestMessage;
    private ParsedMessage parsedBookingMessage;
    private MessageSchema schemaTest;
    private MessageSchema schemaBooking;

    @Before
    public void setUp() throws Exception {
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setOrderNumber("booking-order-1")
                .setCustomerTotalFareWithoutSurge(2000L)
                .setAmountPaidByCash(12.3F)
                .setDriverPickupLocation(TestLocation.newBuilder().setLongitude(10.0).setLatitude(23.9).build())
                .build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS")
                .build();
        Message message = new Message(testKey.toByteArray(), testMessage.toByteArray());
        Message bookingMessage = new Message(testKey.toByteArray(),
                testBookingLogMessage.toByteArray());
        Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {
            {
                put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
                put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
                put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
                put(String.format("%s", TestBookingLogMessage.TopicMetadata.class.getName()),
                        TestBookingLogMessage.TopicMetadata.getDescriptor());
                put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
            }
        };
        Parser protoParserTest = StencilClientFactory.getClient().getParser(TestMessage.class.getName());
        parsedTestMessage = new ProtoParsedMessage(protoParserTest.parse((byte[]) message.getLogMessage()));
        Parser protoParserBooking = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        parsedBookingMessage = new ProtoParsedMessage(
                protoParserBooking.parse((byte[]) bookingMessage.getLogMessage()));
        when(sinkConfig.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        ProtoMessageParser messageParser = (ProtoMessageParser) MessageParserFactory.getParser(
                sinkConfig,
                statsDReporter);
        schemaTest = messageParser.getSchema("org.raystack.depot.TestMessage", descriptorsMap);
        schemaBooking = messageParser.getSchema("org.raystack.depot.TestBookingLogMessage", descriptorsMap);
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage, schemaTest));
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s, order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage, schemaTest));
    }

    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,amount_paid_by_cash");
        assertEquals("Test-12.3", template.parse(parsedBookingMessage, schemaBooking));
    }

    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,customer_total_fare_without_surge");
        assertEquals("Test-2000", template.parse(parsedBookingMessage, schemaBooking));
    }

    @Test
    public void shouldThrowExceptionForNullCollectionKeyTemplate() {
        InvalidTemplateException e = assertThrows(InvalidTemplateException.class, () -> new Template(null));
        assertEquals("Template cannot be empty", e.getMessage());
    }

    @Test
    public void shouldThrowExceptionForEmptyCollectionKeyTemplate() {
        InvalidTemplateException e = assertThrows(InvalidTemplateException.class, () -> new Template(""));
        assertEquals("Template cannot be empty", e.getMessage());
    }

    @Test
    public void shouldAcceptStringForCollectionKey() throws InvalidTemplateException {
        Template template = new Template("Test");
        assertEquals("Test", template.parse(parsedBookingMessage, schemaBooking));
    }

    @Test
    public void shouldNotAcceptStringWithPatternForCollectionKeyWithEmptyVariables() {
        InvalidTemplateException e = assertThrows(InvalidTemplateException.class,
                () -> new Template("Test-%s%d%b,t1,t2"));
        Assert.assertEquals("Template is not valid, variables=3, validArgs=3, values=2", e.getMessage());

        e = assertThrows(InvalidTemplateException.class, () -> new Template("Test-%s%s%y,order_number,order_details"));
        Assert.assertEquals("Template is not valid, variables=3, validArgs=2, values=2", e.getMessage());
    }

    @Test
    public void shouldAcceptStringWithPatternForCollectionKeyWithMultipleVariables() throws InvalidTemplateException {
        Template template = new Template("Test-%s::%s, order_number, order_details");
        assertEquals("Test-test-order::ORDER-DETAILS", template.parse(parsedTestMessage, schemaTest));
    }

    @Test
    public void shouldParseComplexObject() throws InvalidTemplateException {
        Template template = new Template("%s,driver_pickup_location");
        String expectedLocation = "{\"name\":\"\",\"address\":\"\",\"latitude\":23.9,\"longitude\":10.0,\"type\":\"\",\"note\":\"\",\"place_id\":\"\",\"accuracy_meter\":0.0,\"gate_id\":\"\"}";
        JSONAssert.assertEquals(expectedLocation, template.parse(parsedBookingMessage, schemaBooking), true);
    }
}
