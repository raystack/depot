package org.raystack.depot.redis.parsers;

import org.raystack.depot.redis.client.entry.RedisEntry;
import org.raystack.depot.message.ParsedMessage;

import java.util.List;

public interface RedisEntryParser {

    List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage);
}
