package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenericJsonSchemaTest {
    private final JSONObject testObj = new JSONObject("{\"intField\":1,\"stringField\":\"value\",\"doubleField\":10.2,\"nested\":{\"a\":1},\"boolField\":true,\"nullField\":null,\"floatField\":1.2E+20,\"f\":1.1E+2}");

    @Test
    public void shouldReturnListOfFields() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        List<SchemaField> fields = jsonSchema.getFields();
        assertEquals(8, fields.size());
        List<String> actual = fields.stream().map(SchemaField::getName).collect(Collectors.toList());
        List<String> expected = Arrays.asList("intField", "stringField", "doubleField", "nested", "boolField", "nullField", "floatField", "f");
        assertTrue(actual.containsAll(expected) && expected.containsAll(actual));
    }

    @Test
    public void shouldReturnFieldSchemaForGivenFieldName() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        SchemaField intField = jsonSchema.getFieldByName("intField");
        assertEquals("intField", intField.getName());
        assertEquals(SchemaFieldType.INT, intField.getType());
    }

    @Test
    public void shouldReturnParsedMessageTypeForNestedFields() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        SchemaField nestedField = jsonSchema.getFieldByName("nested");
        assertTrue(nestedField.getValueType() instanceof GenericJsonSchema);
        assertEquals(1, nestedField.getValueType().getFields().size());
    }

    @Test
    public void shouldAlwaysReturnMessageLogicalType() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        assertEquals(LogicalType.MESSAGE, jsonSchema.logicalType());
    }

}
