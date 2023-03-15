package com.gotocompany.depot.message.field.proto;

import com.google.protobuf.Duration;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.gotocompany.depot.TestMapMessage;
import com.gotocompany.depot.TestMessage;
import org.junit.Assert;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

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
        String expectedJson = "{\"country\":\"japan\",\"city\":\"tokyo\",\"currency\":\"jpy\"}";
        JSONAssert.assertEquals(expectedJson, field1.getString(), true);
        expectedJson = "{\"23\":{\"order_url\":\"\",\"order_number\":\"order1\",\"order_details\":\"\"},\"10\":{\"order_url\":\"\",\"order_number\":\"order2\",\"order_details\":\"\"}}";
        JSONAssert.assertEquals(expectedJson, field2.getString(), true);
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
        String expectedJson = "{\"d1\":\"1234s\",\"d2\":\"1200.000000123s\"}";
        JSONAssert.assertEquals(expectedJson, field1.getString(), true);
    }

    @Test
    public void shouldReturnTimestampMap() {
        TestMapMessage message = TestMapMessage
                .newBuilder()
                .putTimeStamps("ts1", Timestamp.newBuilder().setSeconds(1669962594).build())
                .putTimeStamps("ts2", Timestamp.newBuilder().setSeconds(1669963594).build())
                .build();
        Object timeStamps = message.getField(message.getDescriptorForType().findFieldByName("time_stamps"));
        MapField field1 = new MapField(timeStamps);
        String expectedJson = "{\"ts2\":\"2022-12-02T06:46:34Z\",\"ts1\":\"2022-12-02T06:29:54Z\"}";
        JSONAssert.assertEquals(expectedJson, field1.getString(), true);
    }

    @Test
    public void shouldReturnStructMap() {
        TestMapMessage message = TestMapMessage.newBuilder()
                .putStructMap("test1",
                        Struct.newBuilder().putFields(
                                        "mykey",
                                        Value.newBuilder().setStructValue(
                                                        Struct.newBuilder().putFields("another",
                                                                        Value.newBuilder()
                                                                                .setStringValue("finally")
                                                                                .build())
                                                                .build())
                                                .build())
                                .build())
                .build();

        Object structMap = message.getField(message.getDescriptorForType().findFieldByName("struct_map"));
        MapField field1 = new MapField(structMap);
        Assert.assertEquals("{\"test1\":{\"mykey\":{\"another\":\"finally\"}}}", field1.getString());
    }
}
