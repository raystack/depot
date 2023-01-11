package io.odpf.depot.bigtable.parser;

import io.odpf.depot.common.Template;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigTableRowKeyParser {
    private final Template keyTemplate;
    private final OdpfMessageSchema schema;

    public String parse(ParsedOdpfMessage parsedOdpfMessage) {
        return keyTemplate.parse(parsedOdpfMessage);
    }
}
