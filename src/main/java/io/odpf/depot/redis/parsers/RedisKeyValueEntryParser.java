package io.odpf.depot.redis.parsers;

import io.odpf.depot.common.Template;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisKeyValueEntry;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public class RedisKeyValueEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final String fieldName;
    private final OdpfMessageSchema schema;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage) {
        String redisKey = keyTemplate.parse(parsedOdpfMessage, schema);
        String redisValue = parsedOdpfMessage.getFieldByName(fieldName, schema).toString();
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
