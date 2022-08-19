package io.odpf.depot.redis.parsers;

import com.google.common.base.Splitter;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class RedisParserUtils {
    public static String parseTemplate(String template, ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        if (StringUtils.isEmpty(template)) {
            throw new IllegalArgumentException("Template '" + template + "' is invalid");
        }
        List<String> templateStrings = new ArrayList<>();
        Splitter.on(",").omitEmptyStrings().split(template).forEach(s -> templateStrings.add(s.trim()));
        if (templateStrings.size() == 0) {
            throw new ConfigurationException("Template " + template + " is invalid");
        }
        String templatePattern = templateStrings.get(0);
        List<String> patternVariableFieldNames = templateStrings.subList(1, templateStrings.size());
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
