package io.odpf.depot.redis.parsers;

import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.redis.entry.RedisEntry;

import java.util.List;

public interface RedisEntryParser {

    List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema);
}
