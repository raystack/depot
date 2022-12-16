package io.odpf.depot.config.converter;

import io.odpf.depot.common.Template;
import io.odpf.depot.exception.InvalidTemplateException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TemplateMapConverterTest {

    @Test
    public void shouldConvertToEmptyMapIfInputIsEmpty() {
        TemplateMapConverter converter = new TemplateMapConverter();
        assertEquals(Collections.emptyMap(), converter.convert(null, "{}"));
    }

    @Test
    public void shouldConvertToEmptyMapIfInputIsEmptyString() {
        TemplateMapConverter converter = new TemplateMapConverter();
        assertEquals(Collections.emptyMap(), converter.convert(null, ""));
    }

    @Test
    public void shouldConvertToEmptyMapIfInputIsNull() {
        TemplateMapConverter converter = new TemplateMapConverter();
        assertEquals(Collections.emptyMap(), converter.convert(null, null));
    }

    @Test
    public void shouldConvertToTemplateMap() throws InvalidTemplateException {
        TemplateMapConverter converter = new TemplateMapConverter();
        Template templateKey = new Template("key-%s,order_number");
        Template templateValue = new Template("message-%s,service_type");

        Map<Template, Template> templateMap = converter.convert(null, "{\"key-%s,order_number\":\"message-%s,service_type\"}");
        templateMap.forEach((k, v) -> {
                    assertEquals(templateKey, k);
                    assertEquals(templateValue, v);
                }
        );
    }

    @Test
    public void shouldConvertMultipleTemplateToTemplateMap() throws InvalidTemplateException {
        TemplateMapConverter converter = new TemplateMapConverter();
        Map<Template, Template> expectedTemplateMap = new HashMap<>();
        expectedTemplateMap.put(new Template("key1-%s,order_number"), new Template("message1-%s,service_type"));
        expectedTemplateMap.put(new Template("key2-%s,order_number"), new Template("message2-%s,service_type"));

        Map<Template, Template> templateMap = converter.convert(
                null,
                "{\"key1-%s,order_number\":\"message1-%s,service_type\",\"key2-%s,order_number\":\"message2-%s,service_type\"}");
        assertEquals(expectedTemplateMap, templateMap);
    }
}
