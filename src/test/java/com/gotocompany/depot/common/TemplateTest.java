package com.gotocompany.depot.common;

import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import com.gotocompany.stencil.Parser;
import com.gotocompany.stencil.StencilClientFactory;
import com.jayway.jsonpath.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TemplateTest {
    private ParsedMessage parsedTestMessage;
    private ParsedMessage parsedBookingMessage;
    @Mock
    private SinkConfig sinkConfig;

    @Before
    public void setUp() throws Exception {
        sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkDefaultFieldValueEnable()).thenReturn(false);
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
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
        Parser protoParserTest = StencilClientFactory.getClient().getParser(TestMessage.class.getName());
        parsedTestMessage = new ProtoParsedMessage(protoParserTest.parse((byte[]) message.getLogMessage()), jsonPathConfig);
        Parser protoParserBooking = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        parsedBookingMessage = new ProtoParsedMessage(protoParserBooking.parse((byte[]) bookingMessage.getLogMessage()), jsonPathConfig);
    }

    @Test
    public void shouldParseStringMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage));
    }

    @Test
    public void shouldParseStringMessageWithSpacesForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s, order_number");
        assertEquals("Test-test-order", template.parse(parsedTestMessage));
    }

    @Test
    public void shouldParseFloatMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,amount_paid_by_cash");
        assertEquals("Test-12.3", template.parse(parsedBookingMessage));
    }

    @Test
    public void shouldParseLongMessageForCollectionKeyTemplate() throws InvalidTemplateException {
        Template template = new Template("Test-%s,customer_total_fare_without_surge");
        assertEquals("Test-2000", template.parse(parsedBookingMessage));
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
        assertEquals("Test", template.parse(parsedBookingMessage));
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
        assertEquals("Test-test-order::ORDER-DETAILS", template.parse(parsedTestMessage));
    }

    @Test
    public void shouldParseComplexObject() throws InvalidTemplateException {
        Template template = new Template("%s,driver_pickup_location");
        String expectedLocation = "{\"latitude\":23.9,\"longitude\":10.0}";
        JSONAssert.assertEquals(expectedLocation, template.parse(parsedBookingMessage), true);
    }

    @Test
    public void shouldGetTemplateStringFromConstantString() throws InvalidTemplateException {
        Template template = new Template("http://dummy.com");
        assertEquals("http://dummy.com", template.getTemplateString());
    }

    @Test
    public void shouldGetTemplatePatternFromParameterizedString() throws InvalidTemplateException {
        Template template = new Template("http://dummy.com/%s,order_number");
        assertEquals("http://dummy.com/%s", template.getTemplateString());
    }
}
