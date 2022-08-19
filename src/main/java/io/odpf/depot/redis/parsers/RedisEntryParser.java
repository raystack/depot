package io.odpf.depot.redis.parsers;

import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.redis.dataentry.RedisDataEntry;

import java.util.List;

public interface RedisEntryParser {

    List<RedisDataEntry> getRedisEntry(ParsedOdpfMessage parsedOdpfMessage, OdpfMessageSchema schema);
}
