package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Enclosed.class)
public class ProtoSchemaFieldTest {
    private static final Descriptors.Descriptor TEST_SCHEMA = TestTypesMessage.getDescriptor();

    public static class SingleRunTests {

        @Test
        public void getValueTypeShouldReturnNullIfFieldIsNotMessageType() {
            Descriptors.FieldDescriptor stringField = TEST_SCHEMA.findFieldByName("string_value");
            ProtoSchemaField schemaField = new ProtoSchemaField(stringField);
            assertNull(schemaField.getValueType());
        }

        @Test
        public void getValueTypeShouldReturnSchemaOfMessageType() {
            Descriptors.FieldDescriptor stringField = TEST_SCHEMA.findFieldByName("message_value");
            ProtoSchemaField schemaField = new ProtoSchemaField(stringField);
            assertNotNull(schemaField.getValueType());
            assertEquals("com.gotocompany.depot.TestMessage", schemaField.getValueType().getFullName());
        }

    }

    @RunWith(Parameterized.class)
    public static class SchemaFieldTypeTest {
        private final Descriptors.Descriptor testMessageDescriptor = TestTypesMessage.getDescriptor();
        private String fieldName;
        private SchemaFieldType expectedType;
        private boolean repeated;

        public SchemaFieldTypeTest(String fieldName, SchemaFieldType expectedType, boolean repeated) {
            this.fieldName = fieldName;
            this.expectedType = expectedType;
            this.repeated = repeated;
        }

        @Parameterized.Parameters(name = "{index}: {0} should return type {1}. isRepeated: {2}")
        public static Iterable<Object[]> testCases() {
            return Arrays.asList(new Object[][]{
                    {"double_value", SchemaFieldType.DOUBLE, false},
                    {"bytes_value", SchemaFieldType.BYTES, false},
                    {"float_value", SchemaFieldType.FLOAT, false},
                    {"int32_value", SchemaFieldType.INT, false},
                    {"int64_value", SchemaFieldType.LONG, false},
                    {"uint32_value", SchemaFieldType.INT, false},
                    {"uint64_value", SchemaFieldType.LONG, false},
                    {"fixed32_value", SchemaFieldType.INT, false},
                    {"fixed64_value", SchemaFieldType.LONG, false},
                    {"sfixed32_value", SchemaFieldType.INT, false},
                    {"sfixed64_value", SchemaFieldType.LONG, false},
                    {"sint32_value", SchemaFieldType.INT, false},
                    {"sint64_value", SchemaFieldType.LONG, false},
                    {"enum_value", SchemaFieldType.ENUM, false},
                    {"string_value", SchemaFieldType.STRING, false},
                    {"message_value", SchemaFieldType.MESSAGE, false},
                    {"bool_value", SchemaFieldType.BOOLEAN, false},
                    {"duration_value", SchemaFieldType.MESSAGE, false},
                    {"list_values", SchemaFieldType.STRING, true},
                    {"list_message_values", SchemaFieldType.MESSAGE, true}
            });
        }

        @Test
        public void testSchemaTypeMapping() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(expectedType, schemaField.getType());
        }

        @Test
        public void testGetName() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(fieldName, schemaField.getName());
        }

        @Test
        public void testGetJsonName() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(doubleField.getJsonName(), schemaField.getJsonName());
        }

        @Test
        public void testIsRepeated() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(repeated, schemaField.isRepeated());
        }
    }
}
