package org.raystack.depot.bigtable.parser;

import org.raystack.depot.common.Template;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigTableRowKeyParser {
    private final Template keyTemplate;
    private final RaystackMessageSchema schema;

    public String parse(ParsedRaystackMessage parsedRaystackMessage) {
        return keyTemplate.parse(parsedRaystackMessage, schema);
    }
}
