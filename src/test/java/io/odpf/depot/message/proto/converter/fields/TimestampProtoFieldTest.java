package org.raystack.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.raystack.depot.TestDurationMessage;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimestampProtoFieldTest {
    private TimestampProtoField timestampProtoField;
    private Instant time;

    @Before
    public void setUp() throws Exception {
        time = Instant.ofEpochSecond(200, 200);
        TestDurationMessage message = TestDurationMessage.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder()
                        .setSeconds(time.getEpochSecond())
                        .setNanos(time.getNano())
                        .build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType()
                .findFieldByName("event_timestamp");
        timestampProtoField = new TimestampProtoField(fieldDescriptor, dynamicMessage.getField(fieldDescriptor));
    }

    @Test
    public void shouldParseGoogleProtobufTimestampProtoMessageToInstant() throws InvalidProtocolBufferException {
        assertEquals(time, timestampProtoField.getValue());
    }

    @Test
    public void shouldMatchGoogleProtobufTimestamp() {
        assertTrue(timestampProtoField.matches());
    }
}
