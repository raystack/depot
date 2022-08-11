package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.models.RedisRecords;
import lombok.AllArgsConstructor;
import io.odpf.depot.message.ParsedOdpfMessage;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;



/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {
    private OdpfMessageParser odpfMessageParser;
    private RedisSinkConfig redisSinkConfig;

    public abstract List<RedisDataEntry> parseRedisEntry(ParsedOdpfMessage parsedOdpfMessage, String redisKey, OdpfMessageSchema schema);

    String parseKeyTemplate(String template, ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        if (StringUtils.isEmpty(template)) {
            throw new ConfigurationException("Empty config SINK_REDIS_KEY_TEMPLATE found");
        }
        String[] templateStrings = template.split(",");
        if (templateStrings.length == 0) {
            throw new ConfigurationException("Invalid key configuration SINK_REDIS_KEY_TEMPLATE: '" + template + "'");
        }
        templateStrings = Arrays
                .stream(templateStrings)
                .map(String::trim)
                .toArray(String[]::new);
        String templatePattern = templateStrings[0];
        String templateVariables = StringUtils.join(Arrays.copyOfRange(templateStrings, 1, templateStrings.length), ",");
        String renderedTemplate = renderStringTemplate(parsedOdpfMessage, schema, templatePattern, templateVariables);
        return StringUtils.isEmpty(templateVariables)
                ? templatePattern
                : renderedTemplate;
    }

    private String renderStringTemplate(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema, String pattern, String patternVariables) {
        if (StringUtils.isEmpty(patternVariables)) {
            return pattern;
        }
        List<String> patternVariableFieldNames = Arrays.asList(patternVariables.split(","));
        Object[] patternVariableData = patternVariableFieldNames
                .stream()
                .map(fieldName -> parsedOdpfMessage.getFieldByName(fieldName, schema))
                .toArray();
        return String.format(pattern, patternVariableData);
    }

    public RedisRecords convert(List<OdpfMessage> messages) {
        List<RedisRecord> valid = new ArrayList<>();
        List<RedisRecord> invalid = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            try {
                SinkConnectorSchemaMessageMode mode = redisSinkConfig.getSinkConnectorSchemaMessageMode();
                String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                        ? redisSinkConfig.getSinkConnectorSchemaProtoMessageClass() : redisSinkConfig.getSinkConnectorSchemaProtoKeyClass();
                OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass);
                ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(messages.get(i), mode, schemaClass);
                String redisKey = parseKeyTemplate(redisSinkConfig.getSinkRedisKeyTemplate(), parsedOdpfMessage, schema);
                List<RedisDataEntry> p = parseRedisEntry(parsedOdpfMessage, redisKey, schema);
                for (int ii = 0; ii < p.size(); ii++) {
                    valid.add(new RedisRecord(p.get(ii), (long) valid.size(), new ErrorInfo(null, null)));
                }
            } catch (ConfigurationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                invalid.add(new RedisRecord(null, (long) i, errorInfo));
            } catch (DeserializerException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                invalid.add(new RedisRecord(null, (long) i, errorInfo));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new RedisRecords(valid, invalid);
    }
}
