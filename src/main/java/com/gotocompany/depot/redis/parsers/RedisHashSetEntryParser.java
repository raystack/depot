package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.message.field.GenericFieldFactory;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisHashSetFieldEntry;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Redis hash set parser.
 */
@AllArgsConstructor
public class RedisHashSetEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final Map<String, Template> fieldTemplates;
    private final MessageSchema schema;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage) {
        String redisKey = keyTemplate.parse(parsedMessage, schema);
        return fieldTemplates
                .entrySet()
                .stream()
                .map(fieldTemplate -> {
                    String field = fieldTemplate.getValue().parse(parsedMessage, schema);
                    String redisValue = GenericFieldFactory.getField(parsedMessage.getFieldByName(fieldTemplate.getKey(), schema)).getString();
                    return new RedisHashSetFieldEntry(redisKey, field, redisValue, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
                }).collect(Collectors.toList());
    }
}
