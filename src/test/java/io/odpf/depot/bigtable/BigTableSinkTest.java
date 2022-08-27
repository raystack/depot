package io.odpf.depot.bigtable;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.TestBookingLogKey;
import io.odpf.depot.TestBookingLogMessage;
import io.odpf.depot.TestServiceType;
import io.odpf.depot.bigtable.client.BigTableClient;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.parser.BigTableRecordParser;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.message.OdpfMessage;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

public class BigTableSinkTest {

    @Mock
    private BigTableRecordParser bigTableRecordParser;
    @Mock
    private BigTableClient bigTableClient;

    private BigTableSink bigTableSink;
    private List<OdpfMessage> messages;
    private List<BigTableRecord> validRecords;
    private List<BigTableRecord> invalidRecords;
    private ErrorInfo errorInfo;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

        OdpfMessage message1 = new OdpfMessage(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        OdpfMessage message2 = new OdpfMessage(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());
        messages = Collections.list(message1, message2);

        RowMutationEntry rowMutationEntry = RowMutationEntry.create("rowKey").setCell("family", "qualifier", "value");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry, 1, null, true);
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry, 2, null, true);
        validRecords = Collections.list(bigTableRecord1, bigTableRecord2);

        errorInfo = new ErrorInfo(new Exception("test-exception-message"), ErrorType.DEFAULT_ERROR);
        BigTableRecord bigTableRecord3 = new BigTableRecord(null, 3, errorInfo, false);
        BigTableRecord bigTableRecord4 = new BigTableRecord(null, 4, errorInfo, false);
        invalidRecords = Collections.list(bigTableRecord3, bigTableRecord4);

        bigTableSink = new BigTableSink(bigTableClient, bigTableRecordParser);
    }

    @Test
    public void shouldSendValidBigTableRecordsToBigTableSink() {
        Mockito.when(bigTableRecordParser.convert(messages)).thenReturn(validRecords);
        Mockito.when(bigTableClient.send(validRecords)).thenReturn(null);

        OdpfSinkResponse response = bigTableSink.pushToSink(messages);

        Mockito.verify(bigTableClient, Mockito.times(1)).send(validRecords);
        Assert.assertEquals(0, response.getErrors().size());
    }

    @Test
    public void shouldAddErrorsFromInvalidRecordsToOdpfResponse() {
        Mockito.when(bigTableRecordParser.convert(messages)).thenReturn(invalidRecords);

        OdpfSinkResponse response = bigTableSink.pushToSink(messages);

        Mockito.verify(bigTableClient, Mockito.times(0)).send(validRecords);
        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(2, response.getErrors().size());
        Assert.assertEquals(errorInfo, response.getErrorsFor(3));
        Assert.assertEquals(errorInfo, response.getErrorsFor(4));
    }
}
