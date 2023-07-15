package org.raystack.depot.redis.parsers;

import org.raystack.depot.message.ParsedOdpfMessage;
import org.raystack.depot.redis.client.entry.RedisEntry;

import java.util.List;

public interface RedisEntryParser {

    List<RedisEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage);
}
