package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMapMessage;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProtoSchemaTest {
    private final Descriptors.Descriptor testMessageDescriptor = TestTypesMessage.getDescriptor();
    private Schema schema = new ProtoSchema(testMessageDescriptor);

    @Test
    public void shouldReturnFullName() {
        assertEquals("com.gotocompany.depot.TestTypesMessage", schema.getFullName());
    }

    @Test
    public void shouldReturnAllFieldsPresentInProtoSchema() {
        List<SchemaField> fields = schema.getFields();
        assertEquals(24, fields.size());
    }

    @Test
    public void shouldReturnSchemaFieldGivenAFieldName() {
        SchemaField sint32Field = schema.getFieldByName("sint32_value");
        assertEquals(SchemaFieldType.INT, sint32Field.getType());
    }

    @Test
    public void shouldReturnTimestampLogicalTypeForTimestampFields() {
        SchemaField timestampField = schema.getFieldByName("timestamp_value");
        assertEquals(SchemaFieldType.MESSAGE, timestampField.getType());
        assertEquals(LogicalType.TIMESTAMP, timestampField.getValueType().logicalType());
    }

    @Test
    public void shouldReturnDurationLogicalTypeForDurationField() {
        SchemaField durationField = schema.getFieldByName("duration_value");
        assertEquals(SchemaFieldType.MESSAGE, durationField.getType());
        assertEquals(LogicalType.DURATION, durationField.getValueType().logicalType());
    }

    @Test
    public void shouldReturnStructLogicalTypeForStructFields() {
        SchemaField structField = schema.getFieldByName("struct_value");
        assertEquals(SchemaFieldType.MESSAGE, structField.getType());
        assertEquals(LogicalType.STRUCT, structField.getValueType().logicalType());
    }

    @Test
    public void shouldReturnMapLogicalTypeForMapFieldGeneratedMessage() {
        Schema mapSchema = new ProtoSchema(TestMapMessage.getDescriptor());
        SchemaField currentState = mapSchema.getFieldByName("current_state");
        assertEquals(SchemaFieldType.MESSAGE, currentState.getType());
        assertEquals(LogicalType.MAP, currentState.getValueType().logicalType());
        assertEquals(LogicalType.MESSAGE, mapSchema.logicalType());
    }
}
