package org.raystack.depot.redis.parsers;

import org.raystack.depot.common.Template;
import org.raystack.depot.message.field.GenericFieldFactory;
import org.raystack.depot.message.MessageSchema;
import org.raystack.depot.message.ParsedMessage;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.redis.client.entry.RedisKeyValueEntry;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public class RedisKeyValueEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final String fieldName;
    private final MessageSchema schema;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage) {
        String redisKey = keyTemplate.parse(parsedMessage, schema);
        String redisValue = GenericFieldFactory.getField(parsedMessage.getFieldByName(fieldName, schema))
                .getString();
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue,
                new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
