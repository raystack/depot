package org.raystack.depot.bigtable.parser;

import org.raystack.depot.common.Template;
import org.raystack.depot.message.MessageSchema;
import org.raystack.depot.message.ParsedMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigTableRowKeyParser {
    private final Template keyTemplate;
    private final MessageSchema schema;

    public String parse(ParsedMessage parsedMessage) {
        return keyTemplate.parse(parsedMessage, schema);
    }
}
