package io.odpf.depot.redis.parsers;

import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.entry.RedisEntry;
import io.odpf.depot.redis.entry.RedisHashSetFieldEntry;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Redis hash set parser.
 */
public class RedisHashSetEntryParser implements RedisEntryParser {
    private final StatsDReporter statsDReporter;
    private final Template keyTemplate;
    private final Map<String, Template> fieldTemplates;

    public RedisHashSetEntryParser(StatsDReporter statsDReporter, Template keyTemplate, Map<String, Template> fieldTemplates) {
        this.statsDReporter = statsDReporter;
        this.keyTemplate = keyTemplate;
        this.fieldTemplates = fieldTemplates;
    }

    @Override
    public List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema) {
        String redisKey = keyTemplate.parse(parsedOdpfMessage, schema);
        return fieldTemplates
                .entrySet()
                .stream()
                .map(fieldTemplate -> {
                    String field = fieldTemplate.getValue().parse(parsedOdpfMessage, schema);
                    String redisValue = parsedOdpfMessage.getFieldByName(fieldTemplate.getKey(), schema).toString();
                    return new RedisHashSetFieldEntry(redisKey, field, redisValue, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
                }).collect(Collectors.toList());
    }
}
