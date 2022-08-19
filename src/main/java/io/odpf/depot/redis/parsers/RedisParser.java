package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.message.*;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.models.RedisRecords;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;


/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {
    private OdpfMessageParser odpfMessageParser;
    private RedisSinkConfig redisSinkConfig;

    public abstract List<RedisDataEntry> getRedisEntry(long index, ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema);

    String parseKeyTemplate(String template, ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        if (StringUtils.isEmpty(template)) {
            throw new IllegalArgumentException("Template '" + template + "' is invalid");
        }
        String[] templateStrings = template.split(",");
        if (templateStrings.length == 0) {
            throw new ConfigurationException("Template " + template + " is invalid");
        }
        templateStrings = Arrays
                .stream(templateStrings)
                .map(String::trim)
                .toArray(String[]::new);
        String templatePattern = templateStrings[0];
        List<String> patternVariableFieldNames = Arrays.asList(templateStrings).subList(1, templateStrings.length);
        if (patternVariableFieldNames.isEmpty()) {
            return templatePattern;
        }
        Object[] patternVariableData = patternVariableFieldNames
                .stream()
                .map(fieldName -> parsedOdpfMessage.getFieldByName(fieldName, schema))
                .toArray();
        return String.format(templatePattern, patternVariableData);
    }

    public RedisRecords convert(List<OdpfMessage> messages) {
        List<RedisRecord> valid = new ArrayList<>();
        List<RedisRecord> invalid = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            try {
                SinkConnectorSchemaMessageMode mode = redisSinkConfig.getSinkConnectorSchemaMessageMode();
                String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                        ? redisSinkConfig.getSinkConnectorSchemaProtoMessageClass() : redisSinkConfig.getSinkConnectorSchemaProtoKeyClass();
                OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass);
                ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(messages.get(index), mode, schemaClass);
                List<RedisDataEntry> p = getRedisEntry(index, parsedOdpfMessage, schema);
                for (RedisDataEntry redisDataEntry : p) {
                    valid.add(new RedisRecord(redisDataEntry, (long) index, null, messages.get(index).getMetadata()));
                }
            } catch (ConfigurationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                invalid.add(new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadata()));
            } catch (DeserializerException | IOException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                invalid.add(new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadata()));
            }
        });
        return new RedisRecords(valid, invalid);
    }
}
