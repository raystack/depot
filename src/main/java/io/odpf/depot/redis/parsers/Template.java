package io.odpf.depot.redis.parsers;

import com.google.common.base.Splitter;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;

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
    }

    public String parse(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        if (patternVariableFieldNames.isEmpty()) {
            return templatePattern;
        }
        Object[] patternVariableData = patternVariableFieldNames
                .stream()
                .map(fieldName -> parsedOdpfMessage.getFieldByName(fieldName, schema))
                .toArray();
        return String.format(templatePattern, patternVariableData);
    }
}
