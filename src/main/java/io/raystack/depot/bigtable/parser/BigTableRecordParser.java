package org.raystack.depot.bigtable.parser;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import org.raystack.depot.bigtable.model.BigTableRecord;
import org.raystack.depot.bigtable.model.BigTableSchema;
import org.raystack.depot.common.Tuple;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.exception.DeserializerException;
import org.raystack.depot.exception.EmptyMessageException;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.message.field.GenericFieldFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class BigTableRecordParser {
    private final RaystackMessageParser raystackMessageParser;
    private final BigTableRowKeyParser bigTableRowKeyParser;
    private final BigTableSchema bigTableSchema;
    private final RaystackMessageSchema schema;
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    public BigTableRecordParser(RaystackMessageParser raystackMessageParser,
            BigTableRowKeyParser bigTableRowKeyParser,
            Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema,
            RaystackMessageSchema schema,
            BigTableSchema bigTableSchema) {
        this.raystackMessageParser = raystackMessageParser;
        this.bigTableRowKeyParser = bigTableRowKeyParser;
        this.modeAndSchema = modeAndSchema;
        this.schema = schema;
        this.bigTableSchema = bigTableSchema;
    }

    public List<BigTableRecord> convert(List<RaystackMessage> messages) {
        ArrayList<BigTableRecord> records = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            RaystackMessage message = messages.get(index);
            BigTableRecord record = createRecord(message, index);
            records.add(record);
        }
        return records;
    }

    private BigTableRecord createRecord(RaystackMessage message, long index) {
        try {
            ParsedRaystackMessage parsedRaystackMessage = raystackMessageParser.parse(message, modeAndSchema.getFirst(),
                    modeAndSchema.getSecond());
            String rowKey = bigTableRowKeyParser.parse(parsedRaystackMessage);
            RowMutationEntry rowMutationEntry = RowMutationEntry.create(rowKey);
            bigTableSchema.getColumnFamilies().forEach(
                    columnFamily -> bigTableSchema
                            .getColumns(columnFamily)
                            .forEach(column -> {
                                String fieldName = bigTableSchema.getField(columnFamily, column);
                                String value = GenericFieldFactory
                                        .getField(parsedRaystackMessage.getFieldByName(fieldName, schema)).getString();
                                rowMutationEntry.setCell(columnFamily, column, value);
                            }));
            BigTableRecord bigTableRecord = new BigTableRecord(rowMutationEntry, index, null, message.getMetadata());
            if (log.isDebugEnabled()) {
                log.debug(bigTableRecord.toString());
            }
            return bigTableRecord;
        } catch (EmptyMessageException e) {
            return createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata());
        } catch (ConfigurationException | IllegalArgumentException e) {
            return createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata());
        } catch (DeserializerException | IOException e) {
            return createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata());
        }
    }

    private BigTableRecord createErrorRecord(Exception e, ErrorType type, long index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        return new BigTableRecord(null, index, errorInfo, metadata);
    }
}
