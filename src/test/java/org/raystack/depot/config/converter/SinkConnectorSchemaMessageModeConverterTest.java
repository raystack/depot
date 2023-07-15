package org.raystack.depot.config.converter;

import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class SinkConnectorSchemaMessageModeConverterTest {

    @Test
    public void shouldConvertLogKey() {
        SinkConnectorSchemaMessageModeConverter converter = new SinkConnectorSchemaMessageModeConverter();
        SinkConnectorSchemaMessageMode mode = converter.convert(null, "LOG_KEY");
        Assert.assertEquals(SinkConnectorSchemaMessageMode.LOG_KEY, mode);
    }

    @Test
    public void shouldThrowException() {
        SinkConnectorSchemaMessageModeConverter converter = new SinkConnectorSchemaMessageModeConverter();
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            converter.convert(null, "Invalid");
        });
        Assert.assertEquals("No enum constant org.raystack.depot.message.SinkConnectorSchemaMessageMode.INVALID",
                exception.getMessage());
    }
}
