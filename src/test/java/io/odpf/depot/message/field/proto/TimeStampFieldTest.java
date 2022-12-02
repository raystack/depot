package io.odpf.depot.message.field.proto;

import com.google.protobuf.Timestamp;
import io.odpf.depot.TestDurationMessage;
import org.junit.Assert;
import org.junit.Test;

public class TimeStampFieldTest {

    @Test
    public void shouldReturnTimeStamps() {
        TestDurationMessage message = TestDurationMessage
                .newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1669962594) .build())
                .build();
        TimeStampField field = new TimeStampField(message.getField(message.getDescriptorForType().findFieldByName("event_timestamp")));
        Assert.assertEquals("2022-12-02T06:29:54Z", field.getString());
    }
}
