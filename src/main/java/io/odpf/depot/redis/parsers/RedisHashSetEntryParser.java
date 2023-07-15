package org.raystack.depot.redis.parsers;

import org.raystack.depot.common.Template;
import org.raystack.depot.message.field.GenericFieldFactory;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.redis.client.entry.RedisHashSetFieldEntry;
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
    private final RaystackMessageSchema schema;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedRaystackMessage parsedRaystackMessage) {
        String redisKey = keyTemplate.parse(parsedRaystackMessage, schema);
        return fieldTemplates
                .entrySet()
                .stream()
                .map(fieldTemplate -> {
                    String field = fieldTemplate.getValue().parse(parsedRaystackMessage, schema);
                    String redisValue = GenericFieldFactory
                            .getField(parsedRaystackMessage.getFieldByName(fieldTemplate.getKey(), schema)).getString();
                    return new RedisHashSetFieldEntry(redisKey, field, redisValue,
                            new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
                }).collect(Collectors.toList());
    }
}
