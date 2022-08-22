package io.odpf.depot.redis.parsers;


import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisListEntry;

import java.util.Collections;
import java.util.List;

/**
 * Redis list parser.
 */
public class RedisListEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final String field;

    public RedisListEntryParser(StatsDReporter statsDReporter, Template keyTemplate, String field) {
        this.statsDReporter = statsDReporter;
        this.keyTemplate = keyTemplate;
        this.field = field;
    }

    @Override
    public List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = keyTemplate.parse(parsedOdpfMessage, schema);
        String redisValue = parsedOdpfMessage.getFieldByName(field, schema).toString();
        return Collections.singletonList(new RedisListEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisListEntry.class)));
    }
}
