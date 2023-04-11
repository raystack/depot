package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisKeyValueEntry;
import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public class RedisKeyValueEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final String fieldName;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage) {
        String redisKey = keyTemplate.parse(parsedMessage);
        String redisValue = parsedMessage.getFieldByName(fieldName).toString();
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisKeyValueEntry.class));
        return Collections.singletonList(redisKeyValueEntry);
    }
}
