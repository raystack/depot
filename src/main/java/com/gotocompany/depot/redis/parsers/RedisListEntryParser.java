package com.gotocompany.depot.redis.parsers;


import com.gotocompany.depot.message.field.GenericFieldFactory;
import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.entry.RedisListEntry;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
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
    private final MessageSchema schema;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage) {
        String redisKey = keyTemplate.parse(parsedMessage);
        String redisValue = GenericFieldFactory.getField(parsedMessage.getFieldByName(field)).getString();
        return Collections.singletonList(new RedisListEntry(redisKey, redisValue, new Instrumentation(statsDReporter, RedisListEntry.class)));
    }
}
