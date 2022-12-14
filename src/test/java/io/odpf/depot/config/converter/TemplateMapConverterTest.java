package io.odpf.depot.config.converter;

import io.odpf.depot.common.Template;
import org.junit.Test;

import java.util.Collections;
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
    public void shouldConvertToTemplateMap() {
        TemplateMapConverter converter = new TemplateMapConverter();

        Map<Template, Template> templateMap = converter.convert(null, "{\"key-%s,order_number\":\"message-%s,service_type\"}");
        templateMap.forEach((k, v) -> {
                    assertEquals(k.getTemplatePattern(), "key-%s");
                    assertEquals(k.getPatternVariableFieldNames(), Collections.singletonList("order_number"));
                    assertEquals(v.getTemplatePattern(), "message-%s");
                    assertEquals(v.getPatternVariableFieldNames(), Collections.singletonList("service_type"));
                }
        );
    }
}
