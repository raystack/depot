package io.odpf.depot.bigtable.parser;

import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.message.OdpfMessage;
import org.junit.Test;

import static org.junit.Assert.*;


public class BigTableRowKeyParserTest {

    @Test
    public void shouldReturnParsedRowKey() {
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser();
        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        String parsedRowKey = bigTableRowKeyParser.parse("template", new OdpfMessage(bookingLogKey1, bookingLogMessage1));
        assertTrue(parsedRowKey.contains("key-test-"));
    }
}
