package org.raystack.depot.redis.parsers;

import org.raystack.depot.common.Template;
import org.raystack.depot.message.field.GenericFieldFactory;
import org.raystack.depot.message.OdpfMessageSchema;
import org.raystack.depot.message.ParsedOdpfMessage;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.redis.client.entry.RedisListEntry;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * Redis list parser.
 */
@AllArgsConstructor
public class RedisListEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final String field;
    private final OdpfMessageSchema schema;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage) {
        String redisKey = keyTemplate.parse(parsedOdpfMessage, schema);
        String redisValue = GenericFieldFactory.getField(parsedOdpfMessage.getFieldByName(field, schema)).getString();
        return Collections.singletonList(
                new RedisListEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisListEntry.class)));
    }
}
