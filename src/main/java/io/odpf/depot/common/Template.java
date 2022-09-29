package io.odpf.depot.common;

import com.google.common.base.Splitter;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class Template {
    private final String templatePattern;
    private final List<String> patternVariableFieldNames;

    public Template(String template) {
        if (template == null || template.isEmpty()) {
            throw new IllegalArgumentException("Template '" + template + "' is invalid");
        }
        List<String> templateStrings = new ArrayList<>();
        Splitter.on(",").omitEmptyStrings().split(template).forEach(s -> templateStrings.add(s.trim()));
        this.templatePattern = templateStrings.get(0);
        this.patternVariableFieldNames = templateStrings.subList(1, templateStrings.size());
        validate();
    }

    private void validate() {
        int validArgs = StringUtils.countVariables(templatePattern);
        int values = patternVariableFieldNames.size();
        int variables = StringUtils.count(templatePattern, '%');
        if (validArgs != values || variables != values) {
            throw new IllegalArgumentException(String.format("Template is not valid, variables=%d, validArgs=%d, values=%d", variables, validArgs, values));
        }
    }

    public String parse(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        Object[] patternVariableData = patternVariableFieldNames
                .stream()
                .map(fieldName -> parsedOdpfMessage.getFieldByName(fieldName, schema))
                .toArray();
        return String.format(templatePattern, patternVariableData);
    }
}
