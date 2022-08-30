package io.odpf.depot.redis.parsers;


import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisListEntry;
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
        String redisValue = parsedOdpfMessage.getFieldByName(field, schema).toString();
        return Collections.singletonList(new RedisListEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisListEntry.class)));
    }
}
