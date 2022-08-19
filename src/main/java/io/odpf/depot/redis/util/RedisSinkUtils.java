package io.odpf.depot.redis.util;

import com.google.common.base.Splitter;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.redis.client.response.RedisResponse;
import io.odpf.depot.redis.record.RedisRecord;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RedisSinkUtils {
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

    public static Map<Long, ErrorInfo> getErrorsFromResponse(List<RedisRecord> redisRecords, List<RedisResponse> responses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> {
                    RedisResponse response = responses.get(index);
                    if (response.isFailed()) {
                        RedisRecord record = redisRecords.get(index);
                        instrumentation.logError("Error while inserting to redis for message. Record: {}, Error: {}",
                                record.toString(), response.getMessage());
                        errors.put(record.getIndex(), new ErrorInfo(new Exception(response.getMessage()), ErrorType.DEFAULT_ERROR));
                    }
                }
        );
        return errors;
    }
}
