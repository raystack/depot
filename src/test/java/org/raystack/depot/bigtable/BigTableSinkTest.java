package org.raystack.depot.bigtable;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import org.raystack.depot.bigtable.client.BigTableClient;
import org.raystack.depot.bigtable.model.BigTableRecord;
import org.raystack.depot.bigtable.parser.BigTableRecordParser;
import org.raystack.depot.SinkResponse;
import org.raystack.depot.TestBookingLogKey;
import org.raystack.depot.TestBookingLogMessage;
import org.raystack.depot.TestServiceType;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.message.Message;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.metrics.BigTableMetrics;
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
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private BigTableMetrics bigtableMetrics;

    private BigTableSink bigTableSink;
    private List<Message> messages;
    private List<BigTableRecord> validRecords;
    private List<BigTableRecord> invalidRecords;
    private ErrorInfo errorInfo;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1")
                .setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1")
                .setOrderUrl("order-url#1").setServiceType(TestServiceType.Enum.GO_SEND).build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2")
                .setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2")
                .setOrderUrl("order-url#2").setServiceType(TestServiceType.Enum.GO_SHOP).build();

        Message message1 = new Message(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        Message message2 = new Message(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());
        messages = Collections.list(message1, message2);

        RowMutationEntry rowMutationEntry = RowMutationEntry.create("rowKey").setCell("family", "qualifier", "value");
        BigTableRecord bigTableRecord1 = new BigTableRecord(rowMutationEntry, 1, null, message1.getMetadata());
        BigTableRecord bigTableRecord2 = new BigTableRecord(rowMutationEntry, 2, null, message2.getMetadata());
        validRecords = Collections.list(bigTableRecord1, bigTableRecord2);

        errorInfo = new ErrorInfo(new Exception("test-exception-message"), ErrorType.DEFAULT_ERROR);
        BigTableRecord bigTableRecord3 = new BigTableRecord(null, 3, errorInfo, message1.getMetadata());
        BigTableRecord bigTableRecord4 = new BigTableRecord(null, 4, errorInfo, message2.getMetadata());
        invalidRecords = Collections.list(bigTableRecord3, bigTableRecord4);

        bigTableSink = new BigTableSink(bigTableClient, bigTableRecordParser, bigtableMetrics,
                new Instrumentation(statsDReporter, BigTableSink.class));
    }

    @Test
    public void shouldSendValidBigTableRecordsToBigTableSink() {
        Mockito.when(bigTableRecordParser.convert(messages)).thenReturn(validRecords);
        Mockito.when(bigTableClient.send(validRecords)).thenReturn(null);

        SinkResponse response = bigTableSink.pushToSink(messages);

        Mockito.verify(bigTableClient, Mockito.times(1)).send(validRecords);
        Assert.assertEquals(0, response.getErrors().size());
    }

    @Test
    public void shouldAddErrorsFromInvalidRecordsToResponse() {
        Mockito.when(bigTableRecordParser.convert(messages)).thenReturn(invalidRecords);

        SinkResponse response = bigTableSink.pushToSink(messages);

        Mockito.verify(bigTableClient, Mockito.times(0)).send(validRecords);
        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(2, response.getErrors().size());
        Assert.assertEquals(errorInfo, response.getErrorsFor(3));
        Assert.assertEquals(errorInfo, response.getErrorsFor(4));
    }
}
