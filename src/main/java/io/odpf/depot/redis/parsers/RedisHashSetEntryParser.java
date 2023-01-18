package io.odpf.depot.redis.parsers;

import io.odpf.depot.common.Template;
import io.odpf.depot.message.field.GenericFieldFactory;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.redis.client.entry.RedisEntry;
import io.odpf.depot.redis.client.entry.RedisHashSetFieldEntry;
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
    private final OdpfMessageSchema schema;

    @Override
    public List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage) {
        String redisKey = keyTemplate.parse(parsedOdpfMessage);
        return fieldTemplates
                .entrySet()
                .stream()
                .map(fieldTemplate -> {
                    String field = fieldTemplate.getValue().parse(parsedOdpfMessage);
                    String redisValue = GenericFieldFactory.getField(parsedOdpfMessage.getFieldByName(fieldTemplate.getKey())).getString();
                    return new RedisHashSetFieldEntry(redisKey, field, redisValue, new Instrumentation(statsDReporter, RedisHashSetFieldEntry.class));
                }).collect(Collectors.toList());
    }
}
