package io.odpf.depot.message.field.proto;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.odpf.depot.TestMapMessage;
import io.odpf.depot.TestMessage;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class MapFieldTest {

    @Test
    public void shouldReturnMap() {
        TestMessage metadata1 = TestMessage.newBuilder().setOrderNumber("order1").build();
        TestMessage metadata2 = TestMessage.newBuilder().setOrderNumber("order2").build();
        TestMapMessage message = TestMapMessage
                .newBuilder()
                .putCurrentState("country", "japan")
                .putCurrentState("city", "tokyo")
                .putCurrentState("currency", "jpy")
                .putMetadata(23, metadata1)
                .putMetadata(10, metadata2)
                .build();
        Object currentState = message.getField(message.getDescriptorForType().findFieldByName("current_state"));
        Object metadata = message.getField(message.getDescriptorForType().findFieldByName("metadata"));
        MapField field1 = new MapField(currentState);
        MapField field2 = new MapField(metadata);
        Assert.assertEquals(
                new JSONObject("{\"country\":\"japan\",\"city\":\"tokyo\",\"currency\":\"jpy\"}").toString(),
                new JSONObject(field1.getString()).toString());
        Assert.assertEquals(
                new JSONObject("{\"23\":\"{\\\"order_number\\\":\\\"order1\\\",\\\"order_url\\\":\\\"\\\",\\\"order_details\\\":\\\"\\\"}\",\"10\":\"{\\\"order_number\\\":\\\"order2\\\",\\\"order_url\\\":\\\"\\\",\\\"order_details\\\":\\\"\\\"}\"}").toString(),
                new JSONObject(field2.getString()).toString());
    }

    @Test
    public void shouldReturnDurationMap() {
        TestMapMessage message = TestMapMessage
                .newBuilder()
                .putDurations("d1", Duration.newBuilder().setSeconds(1234).build())
                .putDurations("d2", Duration.newBuilder().setSeconds(1200).setNanos(123).build())
                .build();
        Object durations = message.getField(message.getDescriptorForType().findFieldByName("durations"));
        MapField field1 = new MapField(durations);
        Assert.assertEquals(
                new JSONObject("{\"d1\":\"1234s\",\"d2\":\"1200.000000123s\"}").toString(),
                new JSONObject(field1.getString()).toString());
    }

    @Test
    public void shouldReturnTimestampMap() {
        TestMapMessage message = TestMapMessage
                .newBuilder()
                .putTimeStamps("ts1", Timestamp.newBuilder().setSeconds(1669962594).build())
                .putTimeStamps("ts2", Timestamp.newBuilder().setSeconds(1669963594).build())
                .build();
        Object timeStamps = message.getField(message.getDescriptorForType().findFieldByName("timeStamps"));
        MapField field1 = new MapField(timeStamps);
        Assert.assertEquals(
                new JSONObject("{\"ts2\":\"2022-12-02T06:46:34Z\",\"ts1\":\"2022-12-02T06:29:54Z\"}").toString(),
                new JSONObject(field1.getString()).toString());
    }
}
