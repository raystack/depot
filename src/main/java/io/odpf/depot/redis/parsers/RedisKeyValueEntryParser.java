package io.odpf.depot.redis.parsers;

import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisKeyValueEntry;

import java.util.Collections;
import java.util.List;

public class RedisKeyValueEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final String fieldName;

    public RedisKeyValueEntryParser(StatsDReporter statsDReporter, Template keyTemplate, String fieldName) {
        this.statsDReporter = statsDReporter;
        this.keyTemplate = keyTemplate;
        this.fieldName = fieldName;
    }

    @Override
    public List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = keyTemplate.parse(parsedOdpfMessage, schema);
        String redisValue = parsedOdpfMessage.getFieldByName(fieldName, schema).toString();
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
