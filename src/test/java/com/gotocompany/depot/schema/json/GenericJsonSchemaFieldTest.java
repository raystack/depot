package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
public class GenericJsonSchemaFieldTest {

    private static final JSONObject TEST_OBJ = new JSONObject("{\"intField\":1,\"stringField\":\"value\",\"doubleField\":10.2,\"nested\":{\"a\":1},\"boolField\":true,\"nullField\":null,\"floatField\":2.99792458e8,\"f\":1.1E+2, \"array\": [1,2], \"empty_array\": []}");
    private static final Schema TEST_SCHEMA = new GenericJsonSchema(TEST_OBJ);

    @RunWith(Parameterized.class)
    public static class JSONSchemaFieldTypeTest {
        private final String fieldName;
        private final SchemaFieldType expectedType;
        private final boolean repeated;

        public JSONSchemaFieldTypeTest(String fieldName, SchemaFieldType expectedType, boolean repeated) {
            this.fieldName = fieldName;
            this.expectedType = expectedType;
            this.repeated = repeated;
        }

        @Parameterized.Parameters(name = "{index}: {0} should return type {1}. isRepeated: {2}")
        public static Iterable<Object[]> testCases() {
            return Arrays.asList(new Object[][]{
                    {"intField", SchemaFieldType.INT, false},
                    {"stringField", SchemaFieldType.STRING, false},
                    {"doubleField", SchemaFieldType.DOUBLE, false},
                    {"boolField", SchemaFieldType.BOOLEAN, false},
                    {"floatField", SchemaFieldType.DOUBLE, false},
                    {"nested", SchemaFieldType.MESSAGE, false},
                    {"f", SchemaFieldType.DOUBLE, false},
                    {"array", SchemaFieldType.INT, true},
                    {"empty_array", SchemaFieldType.STRING, true}
            });
        }

        @Test
        public void testSchemaTypeMapping() {
            SchemaField schemaField = TEST_SCHEMA.getFieldByName(fieldName);
            assertEquals(expectedType, schemaField.getType());
        }

        @Test
        public void testIsRepeated() {
            SchemaField schemaField = TEST_SCHEMA.getFieldByName(fieldName);
            assertEquals(repeated, schemaField.isRepeated());
        }

        @Test
        public void testGetJsonName() {
            SchemaField schemaField = TEST_SCHEMA.getFieldByName(fieldName);
            assertEquals(fieldName, schemaField.getJsonName());
        }
    }
}
