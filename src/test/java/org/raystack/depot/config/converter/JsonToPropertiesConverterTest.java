package org.raystack.depot.config.converter;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

import static org.junit.Assert.*;

public class JsonToPropertiesConverterTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldConvertJSONConfigToProperties() {
        String json = "{\"order_number\":\"ORDER_NUMBER\",\"event_timestamp\":\"TIMESTAMP\",\"driver_id\":\"DRIVER_ID\"}";

        Properties properties = new JsonToPropertiesConverter().convert(null, json);

        assertEquals(3, properties.size());
        assertEquals("ORDER_NUMBER", properties.get("order_number"));
        assertEquals("TIMESTAMP", properties.get("event_timestamp"));
        assertEquals("DRIVER_ID", properties.get("driver_id"));
    }

    @Test
    public void shouldValidateJsonConfigForDuplicates() {
        String json = "{\"order_number\":\"ORDER_NUMBER\",\"event_timestamp\":\"TIMESTAMP\",\"driver_id\":\"TIMESTAMP\"}";
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> new JsonToPropertiesConverter().convert(null, json));
        Assert.assertEquals("duplicates found in SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING for : [TIMESTAMP]",
                e.getMessage());
    }

    @Test
    public void shouldValidateJsonConfigForDuplicatesInNestedJsons() {
        String json = "{\"order_number\":\"ORDER_NUMBER\",\"event_timestamp\":\"TIMESTAMP\",\"nested\":{\"1\":\"TIMESTAMP\",\"2\":\"ORDER_NUMBER\"}}";
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> new JsonToPropertiesConverter().convert(null, json));
        String message = e.getMessage();
        String[] actualMessage = (message.split(" : "));
        Assert.assertEquals("duplicates found in SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING for", actualMessage[0]);
        Assert.assertTrue("[ORDER_NUMBER, TIMESTAMP]".equals(actualMessage[1])
                || "[TIMESTAMP, ORDER_NUMBER]".equals(actualMessage[1]));
    }

    @Test
    public void shouldConvertNestedJSONToNestedProperties() {
        String json = "{\"order_id\":{\"order_number\":\"ORDER_NUMBER\",\"order_url\":\"ORDER_URL\",\"order_details\":\"ORDER_DETAILS\"},\"nested_order_details\":\"NUMBER_FIELDS\"}";

        Properties actualProperties = new JsonToPropertiesConverter().convert(null, json);

        Properties expectedNestedProperties = new Properties();
        expectedNestedProperties.put("order_number", "ORDER_NUMBER");
        expectedNestedProperties.put("order_url", "ORDER_URL");
        expectedNestedProperties.put("order_details", "ORDER_DETAILS");

        Properties expectedProperties = new Properties();
        expectedProperties.put("order_id", expectedNestedProperties);
        expectedProperties.put("nested_order_details", "NUMBER_FIELDS");

        assertEquals(actualProperties, expectedProperties);
    }

    @Test
    public void shouldNotProcessEmptyStringAsProperties() {
        String json = "";
        Properties actualProperties = new JsonToPropertiesConverter().convert(null, json);
        assertNull(actualProperties);
    }

    @Test
    public void shouldNotProcessNullStringAsProperties() {
        Properties actualProperties = new JsonToPropertiesConverter().convert(null, null);
        assertNull(actualProperties);
    }
}
