package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.message.*;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.models.RedisRecord;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;


/**
 * Convert Odpf messages to RedisRecords.
 */
@AllArgsConstructor
public class RedisParser {
    private final RedisSinkConfig redisSinkConfig;
    private final OdpfMessageParser odpfMessageParser;
    private final RedisEntryParser redisEntryParser;

    public List<RedisRecord> convert(List<OdpfMessage> messages) {
        List<RedisRecord> records = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            try {
                SinkConnectorSchemaMessageMode mode = redisSinkConfig.getSinkConnectorSchemaMessageMode();
                String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                        ? redisSinkConfig.getSinkConnectorSchemaProtoMessageClass() : redisSinkConfig.getSinkConnectorSchemaProtoKeyClass();
                OdpfMessageSchema schema = odpfMessageParser.getSchema(schemaClass);
                ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(messages.get(index), mode, schemaClass);
                List<RedisDataEntry> redisDataEntries = redisEntryParser.getRedisEntry(parsedOdpfMessage, schema);
                for (RedisDataEntry redisDataEntry : redisDataEntries) {
                    records.add(new RedisRecord(redisDataEntry, (long) index, null, messages.get(index).getMetadataString(), true));
                }
            } catch (ConfigurationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                records.add(new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadataString(), false));
            } catch (DeserializerException | IOException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                records.add(new RedisRecord(null, (long) index, errorInfo, messages.get(index).getMetadataString(), false));
            }
        });
        return records;
    }
}
