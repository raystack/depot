package org.raystack.depot.bigquery;

import com.google.api.client.util.DateTime;
import org.raystack.depot.TestKeyBQ;
import org.raystack.depot.TestMessageBQ;
import org.raystack.depot.common.Tuple;
import org.raystack.depot.message.Message;

import java.sql.Date;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class TestMessageBuilder {
    private long timestamp;
    private String topic;
    private int partition;
    private long offset;
    private long loadTime;

    private TestMessageBuilder() {
    }

    public static TestMessageBuilder withMetadata(TestMetadata testMetadata) {
        TestMessageBuilder builder = new TestMessageBuilder();
        builder.topic = testMetadata.getTopic();
        builder.partition = testMetadata.getPartition();
        builder.offset = testMetadata.getOffset();
        builder.timestamp = testMetadata.getTimestamp();
        builder.loadTime = testMetadata.getLoadTime();
        return builder;
    }

    public Message createConsumerRecord(String orderNumber, String orderUrl, String orderDetails) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .setOrderDetails(orderDetails)
                .build();
        return new Message(
                key.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", topic),
                new Tuple<>("message_partition", partition),
                new Tuple<>("message_offset", offset),
                new Tuple<>("message_timestamp", timestamp),
                new Tuple<>("load_time", loadTime),
                new Tuple<>("should_be_ignored", timestamp));
    }

    public Message createEmptyValueConsumerRecord(String orderNumber, String orderUrl) {
        TestKeyBQ key = TestKeyBQ.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        return new Message(
                key.toByteArray(),
                null,
                new Tuple<>("message_topic", topic),
                new Tuple<>("message_partition", partition),
                new Tuple<>("message_offset", offset),
                new Tuple<>("message_timestamp", timestamp),
                new Tuple<>("load_time", loadTime),
                new Tuple<>("should_be_ignored", timestamp));
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
