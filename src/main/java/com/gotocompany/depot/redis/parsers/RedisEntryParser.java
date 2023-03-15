package com.gotocompany.depot.redis.parsers;

import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.message.ParsedMessage;

import java.util.List;

public interface RedisEntryParser {

    List<RedisEntry> getRedisEntry(ParsedMessage parsedMessage);
}
