package org.raystack.depot.bigquery.client;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import org.raystack.depot.bigquery.TestMetadata;
import org.raystack.depot.bigquery.TestMessageBuilder;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.metrics.BigQueryMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.bigquery.exception.BigQuerySinkException;
import org.raystack.depot.bigquery.models.Record;
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

public class BigQueryResponseParserTest {

    @Mock
    private InsertAllResponse response;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private BigQueryMetrics metrics;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldParseResponse() {
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
        Record record1 = new Record(
                TestMessageBuilder.withMetadata(record1Offset)
                        .createConsumerRecord("order-1", "order-url-1", "order-details-1").getMetadata(),
                new HashMap<>(), 0, null);
        Record record2 = new Record(
                TestMessageBuilder.withMetadata(record2Offset)
                        .createConsumerRecord("order-2", "order-url-2", "order-details-2").getMetadata(),
                new HashMap<>(), 1, null);
        Record record3 = new Record(
                TestMessageBuilder.withMetadata(record3Offset)
                        .createConsumerRecord("order-3", "order-url-3", "order-details-3").getMetadata(),
                new HashMap<>(), 2, null);
        Record record4 = new Record(
                TestMessageBuilder.withMetadata(record4Offset)
                        .createConsumerRecord("order-4", "order-url-4", "order-details-4").getMetadata(),
                new HashMap<>(), 3, null);
        Record record5 = new Record(
                TestMessageBuilder.withMetadata(record5Offset)
                        .createConsumerRecord("order-5", "order-url-5", "order-details-5").getMetadata(),
                new HashMap<>(), 4, null);
        Record record6 = new Record(
                TestMessageBuilder.withMetadata(record6Offset)
                        .createConsumerRecord("order-6", "order-url-6", "order-details-6").getMetadata(),
                new HashMap<>(), 5, null);
        List<Record> records = Collections.list(record1, record2, record3, record4, record5, record6);
        BigQueryError error1 = new BigQueryError("", "US", "");
        BigQueryError error2 = new BigQueryError("invalid", "US", "no such field");
        BigQueryError error3 = new BigQueryError("invalid", "",
                "The destination table's partition tmp$20160101 is outside the allowed bounds. You can only stream to partitions within 1825 days in the past and 366 days in the future relative to the current date");
        BigQueryError error4 = new BigQueryError("stopped", "", "");

        Map<Long, List<BigQueryError>> insertErrorsMap = new HashMap<Long, List<BigQueryError>>() {
            {
                put(0L, Collections.list(error1));
                put(1L, Collections.list(error2));
                put(2L, Collections.list(error3));
                put(3L, Collections.list(error4));
            }
        };
        Mockito.when(response.hasErrors()).thenReturn(true);
        Mockito.when(response.getInsertErrors()).thenReturn(insertErrorsMap);
        Mockito.when(metrics.getBigqueryTotalErrorsMetrics()).thenReturn("test");
        Map<Long, ErrorInfo> errorInfoMap = BigQueryResponseParser.getErrorsFromBQResponse(records, response, metrics,
                instrumentation);

        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR),
                errorInfoMap.get(0L));
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR), errorInfoMap.get(1L));
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR), errorInfoMap.get(2L));
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_5XX_ERROR), errorInfoMap.get(3L));

        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test",
                String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.UNKNOWN_ERROR));
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test", String
                .format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.INVALID_SCHEMA_ERROR));
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test",
                String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.OOB_ERROR));
        Mockito.verify(instrumentation, Mockito.times(1)).incrementCounter("test",
                String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.STOPPED_ERROR));
    }
}
