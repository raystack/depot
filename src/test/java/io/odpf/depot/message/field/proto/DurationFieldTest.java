package io.odpf.depot.message.field.proto;

import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DurationFieldTest {

    @Test
    public void shouldReturnDurationString() throws InvalidProtocolBufferException {
        Duration duration = Duration.newBuilder().setSeconds(1000).setNanos(12123).build();
        DynamicMessage message = DynamicMessage.parseFrom(duration.getDescriptorForType(), duration.toByteArray());
        DurationField field = new DurationField(message);
        Assert.assertEquals("1000.000012123s", field.getString());
    }

    @Test
    public void shouldReturnDurationWithoutNanosString() throws InvalidProtocolBufferException {
        Duration duration = Duration.newBuilder().setSeconds(408).build();
        DynamicMessage message = DynamicMessage.parseFrom(duration.getDescriptorForType(), duration.toByteArray());
        DurationField field = new DurationField(message);
        Assert.assertEquals("408s", field.getString());
    }

    @Test
    public void shouldReturnDurationListString() throws InvalidProtocolBufferException {
        Duration duration1 = Duration.newBuilder().setSeconds(1200).setNanos(1232138).build();
        Duration duration2 = Duration.newBuilder().setSeconds(1300).setNanos(3333434).build();
        Duration duration3 = Duration.newBuilder().setSeconds(1400).setNanos(5665656).build();
        Duration duration4 = Duration.newBuilder().setSeconds(1500).setNanos(9089898).build();
        List<DynamicMessage> messages = new ArrayList<DynamicMessage>() {{
            add(DynamicMessage.parseFrom(duration1.getDescriptorForType(), duration1.toByteArray()));
            add(DynamicMessage.parseFrom(duration2.getDescriptorForType(), duration2.toByteArray()));
            add(DynamicMessage.parseFrom(duration3.getDescriptorForType(), duration3.toByteArray()));
            add(DynamicMessage.parseFrom(duration4.getDescriptorForType(), duration4.toByteArray()));
        }};
        DurationField field = new DurationField(messages);
        Assert.assertEquals("[1200.001232138s,1300.003333434s,1400.005665656s,1500.009089898s]", field.getString());
    }
}
