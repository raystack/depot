package com.gotocompany.depot.common;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import com.gotocompany.stencil.Parser;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.StatsDReporter;
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
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        Message message = new Message(testKey.toByteArray(), testMessage.toByteArray());
        Message bookingMessage = new Message(testKey.toByteArray(), testBookingLogMessage.toByteArray());
        Map<String, Descriptors.Descriptor> descriptorsMap = new HashMap<String, Descriptors.Descriptor>() {{
            put(String.format("%s", TestKey.class.getName()), TestKey.getDescriptor());
            put(String.format("%s", TestMessage.class.getName()), TestMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.class.getName()), TestBookingLogMessage.getDescriptor());
            put(String.format("%s", TestBookingLogMessage.TopicMetadata.class.getName()), TestBookingLogMessage.TopicMetadata.getDescriptor());
            put(String.format("%s", TestLocation.class.getName()), TestLocation.getDescriptor());
        }};
        Parser protoParserTest = StencilClientFactory.getClient().getParser(TestMessage.class.getName());
        parsedTestMessage = new ProtoParsedMessage(protoParserTest.parse((byte[]) message.getLogMessage()));
        Parser protoParserBooking = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        parsedBookingMessage = new ProtoParsedMessage(protoParserBooking.parse((byte[]) bookingMessage.getLogMessage()));
        when(sinkConfig.getSinkConnectorSchemaDataType()).thenReturn(SinkConnectorSchemaDataType.PROTOBUF);
        ProtoMessageParser messageParser = (ProtoMessageParser) MessageParserFactory.getParser(sinkConfig, statsDReporter);
        schemaTest = messageParser.getSchema("com.gotocompany.depot.TestMessage", descriptorsMap);
        schemaBooking = messageParser.getSchema("com.gotocompany.depot.TestBookingLogMessage", descriptorsMap);
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
        InvalidTemplateException e = assertThrows(InvalidTemplateException.class, () -> new Template("Test-%s%d%b,t1,t2"));
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
