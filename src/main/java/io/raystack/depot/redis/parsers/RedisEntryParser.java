package org.raystack.depot.redis.parsers;

import org.raystack.depot.message.ParsedMessage;
import org.raystack.depot.redis.client.entry.RedisEntry;

import java.util.List;

public interface RedisEntryParser {

    List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage);
}
