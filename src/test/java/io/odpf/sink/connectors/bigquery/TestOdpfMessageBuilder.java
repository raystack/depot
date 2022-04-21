package io.odpf.sink.connectors.bigquery;

import com.google.api.client.util.DateTime;
import io.odpf.sink.connectors.TestKeyBQ;
import io.odpf.sink.connectors.TestMessageBQ;
import io.odpf.sink.connectors.config.Tuple;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.message.proto.ProtoOdpfMessage;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class TestOdpfMessageBuilder {
    private long timestamp;
    private String topic;
    private int partition;
    private long offset;
    private long loadTime;

    private TestOdpfMessageBuilder() {
    }

    public static TestOdpfMessageBuilder withMetadata(TestMetadata testMetadata) {
        TestOdpfMessageBuilder builder = new TestOdpfMessageBuilder();
        builder.topic = testMetadata.getTopic();
        builder.partition = testMetadata.getPartition();
        builder.offset = testMetadata.getOffset();
        builder.timestamp = testMetadata.getTimestamp();
        builder.loadTime = testMetadata.getLoadTime();
        return builder;
    }

    public OdpfMessage createConsumerRecord(String orderNumber, String orderUrl, String orderDetails) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .setOrderDetails(orderDetails)
                .build();
        return new ProtoOdpfMessage(
                key.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", topic),
                new Tuple<>("message_partition", partition),
                new Tuple<>("message_offset", offset),
                new Tuple<>("message_timestamp", new DateTime(timestamp)),
                new Tuple<>("load_time", new DateTime(loadTime)));
    }

    public OdpfMessage createEmptyValueConsumerRecord(String orderNumber, String orderUrl) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        return new ProtoOdpfMessage(
                key.toByteArray(),
                null,
                new Tuple<>("message_topic", topic),
                new Tuple<>("message_partition", partition),
                new Tuple<>("message_offset", offset),
                new Tuple<>("message_timestamp", new DateTime(timestamp)),
                new Tuple<>("load_time", new DateTime(loadTime)));
    }

    public static Map<String, Object> metadataColumns(TestMetadata testMetadata, Instant now) {
        Map<String, Object> metadataColumns = new HashMap<>();
        metadataColumns.put("message_partition", testMetadata.getPartition());
        metadataColumns.put("message_offset", testMetadata.getOffset());
        metadataColumns.put("message_topic", testMetadata.getTopic());
        metadataColumns.put("message_timestamp", new DateTime(testMetadata.getTimestamp()));
        metadataColumns.put("load_time", new DateTime(Date.from(now)));
        return metadataColumns;
    }
}
