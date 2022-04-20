package io.odpf.sink.connectors.bigquery.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.sink.connectors.TestMessage;
import io.odpf.sink.connectors.TestNestedMessage;
import io.odpf.sink.connectors.bigquery.converter.fields.NestedProtoField;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NestedProtoFieldTest {

    private NestedProtoField nestedProtoField;
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
        nestedProtoField = new NestedProtoField(fieldDescriptor, dynamicMessage.getField(fieldDescriptor));

    }

    @Test
    public void shouldReturnDynamicMessage() {
        DynamicMessage nestedChild = nestedProtoField.getValue();
        assertEquals(childField, nestedChild);
    }

    @Test
    public void shouldMatchDynamicMessageAsNested() {
        boolean isMatch = nestedProtoField.matches();
        assertTrue(isMatch);
    }
}
