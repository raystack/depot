package io.odpf.depot.message;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

public class MessageUtilsTest {

    @Test
    public void shouldNotThrowExceptionIfValid() throws IOException {
        OdpfMessage message = new OdpfMessage("test", "test");
        MessageUtils.validate(message, String.class);

    }
    @Test
    public void shouldThrowExceptionIfNotValid() {
        OdpfMessage message = new OdpfMessage("test", "test");
        IOException ioException = Assertions.assertThrows(IOException.class, () -> MessageUtils.validate(message, Integer.class));
        Assert.assertEquals("Expected class class java.lang.Integer, but found: LogKey class: class java.lang.String, LogMessage class: class java.lang.String", ioException.getMessage());
    }
}
