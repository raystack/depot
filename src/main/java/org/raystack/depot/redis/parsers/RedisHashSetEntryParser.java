package org.raystack.depot.redis.parsers;

import org.raystack.depot.message.field.GenericFieldFactory;
import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.redis.client.entry.RedisHashSetFieldEntry;
import org.raystack.depot.common.Template;
import org.raystack.depot.message.MessageSchema;
import org.raystack.depot.message.ParsedMessage;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
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
                    String redisValue = GenericFieldFactory
                            .getField(parsedMessage.getFieldByName(fieldTemplate.getKey(), schema)).getString();
                    return new RedisHashSetFieldEntry(redisKey, field, redisValue,
                            new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
                }).collect(Collectors.toList());
    }
}
