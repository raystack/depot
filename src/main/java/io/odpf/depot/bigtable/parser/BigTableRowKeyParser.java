package org.raystack.depot.bigtable.parser;

import org.raystack.depot.common.Template;
import org.raystack.depot.message.OdpfMessageSchema;
import org.raystack.depot.message.ParsedOdpfMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigTableRowKeyParser {
    private final Template keyTemplate;
    private final OdpfMessageSchema schema;

    public String parse(ParsedOdpfMessage parsedOdpfMessage) {
        return keyTemplate.parse(parsedOdpfMessage, schema);
    }
}
