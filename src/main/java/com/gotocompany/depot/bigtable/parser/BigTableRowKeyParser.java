package com.gotocompany.depot.bigtable.parser;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.ParsedMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigTableRowKeyParser {
    private final Template keyTemplate;

    public String parse(ParsedMessage parsedMessage) {
        return keyTemplate.parse(parsedMessage);
    }
}
