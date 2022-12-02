package io.odpf.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.depot.TestMessage;
import io.odpf.depot.TestNestedMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MessageProtoFieldTest {

    private MessageProtoField messageProtoField;
    private TestMessage childField;

    @Before
    public void setUp() throws Exception {
        childField = TestMessage.newBuilder()
                .setOrderNumber("123X")
                .build();
        TestNestedMessage nestedMessage = TestNestedMessage.newBuilder()
                .setSingleMessage(childField)
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(nestedMessage.getDescriptorForType(), nestedMessage.toByteArray());

        Descriptors.FieldDescriptor fieldDescriptor = nestedMessage.getDescriptorForType().findFieldByName("single_message");
        messageProtoField = new MessageProtoField(fieldDescriptor, dynamicMessage.getField(fieldDescriptor));

    }

    @Test
    public void shouldReturnDynamicMessage() {
        DynamicMessage nestedChild = messageProtoField.getValue();
        assertEquals(childField, nestedChild);
    }

    @Test
    public void shouldMatchDynamicMessageAsNested() {
        boolean isMatch = messageProtoField.matches();
        assertTrue(isMatch);
    }
}
