package io.odpf.depot.message;

import java.io.IOException;

public class MessageUtils {
    public static void validate(OdpfMessage message, Class validClass) throws IOException {
        if ((message.getLogKey() != null && !(validClass.isInstance(message.getLogKey())))
                || (message.getLogMessage() != null && !(validClass.isInstance(message.getLogMessage())))) {
            throw new IOException(
                    String.format("Expected class %s, but found: LogKey class: %s, LogMessage class: %s",
                            validClass,
                            message.getLogKey() != null ? message.getLogKey().getClass() : "n/a",
                            message.getLogMessage() != null ? message.getLogMessage().getClass() : "n/a"));
        }
    }
}
