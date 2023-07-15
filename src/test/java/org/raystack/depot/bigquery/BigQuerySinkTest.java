package org.raystack.depot.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import org.raystack.depot.metrics.BigQueryMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.SinkResponse;
import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.bigquery.client.BigQueryRow;
import org.raystack.depot.bigquery.client.BigQueryRowWithInsertId;
import org.raystack.depot.bigquery.handler.ErrorHandler;
import org.raystack.depot.bigquery.converter.MessageRecordConverter;
import org.raystack.depot.bigquery.converter.MessageRecordConverterCache;
import org.raystack.depot.bigquery.models.Record;
import org.raystack.depot.bigquery.models.Records;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.message.Message;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQuerySinkTest {

        private final TableId tableId = TableId.of("test_dataset", "test_table");
        private final MessageRecordConverterCache converterCache = new MessageRecordConverterCache();
        private final BigQueryRow rowCreator = new BigQueryRowWithInsertId(
                        metadata -> metadata.get("topic") + "_" + metadata.get("partition") + "_"
                                        + metadata.get("offset") + "_"
                                        + metadata.get("timestamp"));
        @Mock
        private BigQueryClient client;
        @Mock
        private Instrumentation instrumentation;
        @Mock
        private MessageRecordConverter converter;
        @Mock
        private BigQueryMetrics metrics;
        private BigQuerySink sink;
        @Mock
        private InsertAllResponse insertAllResponse;

        @Mock
        private ErrorHandler errorHandler;

        @Before
        public void setup() {
                MockitoAnnotations.initMocks(this);
                this.converterCache.setMessageRecordConverter(converter);
                this.sink = new BigQuerySink(client, converterCache, rowCreator, metrics, instrumentation,
                                errorHandler);
                Mockito.when(client.getTableID()).thenReturn(tableId);
        }

        @Test
        public void shouldPushToBigQuerySink() {
                TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record3Offset = new TestMetadata("topic1", 3, 103, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record4Offset = new TestMetadata("topic1", 4, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record5Offset = new TestMetadata("topic1", 5, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record6Offset = new TestMetadata("topic1", 6, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                Message message1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord(
                                "order-1",
                                "order-url-1", "order-details-1");
                Message message2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord(
                                "order-2",
                                "order-url-2", "order-details-2");
                Message message3 = TestMessageBuilder.withMetadata(record3Offset).createConsumerRecord(
                                "order-3",
                                "order-url-3", "order-details-3");
                Message message4 = TestMessageBuilder.withMetadata(record4Offset).createConsumerRecord(
                                "order-4",
                                "order-url-4", "order-details-4");
                Message message5 = TestMessageBuilder.withMetadata(record5Offset).createConsumerRecord(
                                "order-5",
                                "order-url-5", "order-details-5");
                Message message6 = TestMessageBuilder.withMetadata(record6Offset).createConsumerRecord(
                                "order-6",
                                "order-url-6", "order-details-6");
                List<Message> messages = Collections.list(message1, message2, message3, message4, message5,
                                message6);
                Record record1 = new Record(message1.getMetadata(), new HashMap<>(), 0, null);
                Record record2 = new Record(message2.getMetadata(), new HashMap<>(), 1, null);
                Record record3 = new Record(message3.getMetadata(), new HashMap<>(), 2, null);
                Record record4 = new Record(message4.getMetadata(), new HashMap<>(), 3, null);
                Record record5 = new Record(message5.getMetadata(), new HashMap<>(), 4, null);
                Record record6 = new Record(message6.getMetadata(), new HashMap<>(), 5, null);
                Records records = new Records(Collections.list(record1, record2, record3, record4, record5, record6),
                                java.util.Collections.emptyList());

                InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
                records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
                InsertAllRequest rows = builder.build();
                Mockito.when(converter.convert(Mockito.eq(messages))).thenReturn(records);
                Mockito.when(client.insertAll(rows)).thenReturn(insertAllResponse);
                Mockito.when(insertAllResponse.hasErrors()).thenReturn(false);
                SinkResponse response = sink.pushToSink(messages);
                Assert.assertEquals(0, response.getErrors().size());
                Mockito.verify(client, Mockito.times(1)).insertAll(rows);
        }

        @Test
        public void shouldReturnInvalidMessages() throws Exception {
                TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record3Offset = new TestMetadata("topic1", 3, 103, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record4Offset = new TestMetadata("topic1", 4, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record5Offset = new TestMetadata("topic1", 5, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record6Offset = new TestMetadata("topic1", 6, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                Message message1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord(
                                "order-1",
                                "order-url-1", "order-details-1");
                Message message2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord(
                                "order-2",
                                "order-url-2", "order-details-2");
                Message message3 = TestMessageBuilder.withMetadata(record3Offset).createConsumerRecord(
                                "order-3",
                                "order-url-3", "order-details-3");
                Message message4 = TestMessageBuilder.withMetadata(record4Offset).createConsumerRecord(
                                "order-4",
                                "order-url-4", "order-details-4");
                Message message5 = TestMessageBuilder.withMetadata(record5Offset).createConsumerRecord(
                                "order-5",
                                "order-url-5", "order-details-5");
                Message message6 = TestMessageBuilder.withMetadata(record6Offset).createConsumerRecord(
                                "order-6",
                                "order-url-6", "order-details-6");
                List<Message> messages = Collections.list(message1, message2, message3, message4, message5,
                                message6);
                Record record1 = new Record(message1.getMetadata(), new HashMap<>(), 0, null);
                Record record2 = new Record(message2.getMetadata(), new HashMap<>(), 1,
                                new ErrorInfo(new RuntimeException(), ErrorType.DEFAULT_ERROR));
                Record record3 = new Record(message3.getMetadata(), new HashMap<>(), 2, null);
                Record record4 = new Record(message4.getMetadata(), new HashMap<>(), 3,
                                new ErrorInfo(new RuntimeException(), ErrorType.INVALID_MESSAGE_ERROR));
                Record record5 = new Record(message5.getMetadata(), new HashMap<>(), 4, null);
                Record record6 = new Record(message6.getMetadata(), new HashMap<>(), 5, null);
                Records records = new Records(Collections.list(record1, record3, record5, record6),
                                Collections.list(record2, record4));

                InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
                records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
                InsertAllRequest rows = builder.build();
                Mockito.when(converter.convert(Mockito.eq(messages))).thenReturn(records);
                Mockito.when(client.insertAll(rows)).thenReturn(insertAllResponse);
                Mockito.when(insertAllResponse.hasErrors()).thenReturn(false);
                SinkResponse response = sink.pushToSink(messages);
                Assert.assertEquals(2, response.getErrors().size());
                Mockito.verify(client, Mockito.times(1)).insertAll(rows);

                Assert.assertEquals(ErrorType.DEFAULT_ERROR, response.getErrors().get(1L).getErrorType());
                Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrors().get(3L).getErrorType());
        }

        @Test
        public void shouldReturnInvalidMessagesWithFailedInsertMessages() throws Exception {
                TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record3Offset = new TestMetadata("topic1", 3, 103, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record4Offset = new TestMetadata("topic1", 4, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record5Offset = new TestMetadata("topic1", 5, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                TestMetadata record6Offset = new TestMetadata("topic1", 6, 104, Instant.now().toEpochMilli(),
                                Instant.now().toEpochMilli());
                Message message1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord(
                                "order-1",
                                "order-url-1", "order-details-1");
                Message message2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord(
                                "order-2",
                                "order-url-2", "order-details-2");
                Message message3 = TestMessageBuilder.withMetadata(record3Offset).createConsumerRecord(
                                "order-3",
                                "order-url-3", "order-details-3");
                Message message4 = TestMessageBuilder.withMetadata(record4Offset).createConsumerRecord(
                                "order-4",
                                "order-url-4", "order-details-4");
                Message message5 = TestMessageBuilder.withMetadata(record5Offset).createConsumerRecord(
                                "order-5",
                                "order-url-5", "order-details-5");
                Message message6 = TestMessageBuilder.withMetadata(record6Offset).createConsumerRecord(
                                "order-6",
                                "order-url-6", "order-details-6");
                List<Message> messages = Collections.list(message1, message2, message3, message4, message5,
                                message6);
                Record record1 = new Record(message1.getMetadata(), new HashMap<>(), 0, null);
                Record record2 = new Record(message2.getMetadata(), new HashMap<>(), 1,
                                new ErrorInfo(new RuntimeException(), ErrorType.DEFAULT_ERROR));
                Record record3 = new Record(message3.getMetadata(), new HashMap<>(), 2, null);
                Record record4 = new Record(message4.getMetadata(), new HashMap<>(), 3,
                                new ErrorInfo(new RuntimeException(), ErrorType.INVALID_MESSAGE_ERROR));
                Record record5 = new Record(message5.getMetadata(), new HashMap<>(), 4, null);
                Record record6 = new Record(message6.getMetadata(), new HashMap<>(), 5, null);
                Records records = new Records(Collections.list(record1, record3, record5, record6),
                                Collections.list(record2, record4));

                InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
                records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
                InsertAllRequest rows = builder.build();
                Mockito.when(converter.convert(Mockito.eq(messages))).thenReturn(records);
                Mockito.when(client.insertAll(rows)).thenReturn(insertAllResponse);
                Mockito.when(insertAllResponse.hasErrors()).thenReturn(true);

                BigQueryError error1 = new BigQueryError("", "US", "");
                BigQueryError error3 = new BigQueryError("invalid", "",
                                "The destination table's partition tmp$20160101 is outside the allowed bounds. You can only stream to partitions within 1825 days in the past and 366 days in the future relative to the current date");

                Map<Long, List<BigQueryError>> insertErrorsMap = new HashMap<Long, List<BigQueryError>>() {
                        {
                                put(0L, Collections.list(error1));
                                put(2L, Collections.list(error3));
                        }
                };
                Mockito.when(insertAllResponse.getInsertErrors()).thenReturn(insertErrorsMap);

                SinkResponse response = sink.pushToSink(messages);
                Mockito.verify(client, Mockito.times(1)).insertAll(rows);
                Mockito.verify(errorHandler, Mockito.times(1)).handle(Mockito.eq(insertErrorsMap), Mockito.any());
                Assert.assertEquals(4, response.getErrors().size());

                Assert.assertEquals(ErrorType.SINK_UNKNOWN_ERROR, response.getErrors().get(0L).getErrorType());
                Assert.assertEquals(ErrorType.DEFAULT_ERROR, response.getErrors().get(1L).getErrorType());
                Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrors().get(3L).getErrorType());
                Assert.assertEquals(ErrorType.SINK_4XX_ERROR, response.getErrors().get(4L).getErrorType());
        }
}
