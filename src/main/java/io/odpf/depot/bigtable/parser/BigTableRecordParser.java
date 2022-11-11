package io.odpf.depot.bigtable.parser;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.model.BigTableSchema;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class BigTableRecordParser {
    private final OdpfMessageParser odpfMessageParser;
    private final BigTableRowKeyParser bigTableRowKeyParser;
    private final BigTableSchema bigTableSchema;
    private final OdpfMessageSchema schema;
    private final Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema;

    public BigTableRecordParser(OdpfMessageParser odpfMessageParser,
                                BigTableRowKeyParser bigTableRowKeyParser,
                                Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema,
                                OdpfMessageSchema schema,
                                BigTableSchema bigTableSchema) {
        this.odpfMessageParser = odpfMessageParser;
        this.bigTableRowKeyParser = bigTableRowKeyParser;
        this.modeAndSchema = modeAndSchema;
        this.schema = schema;
        this.bigTableSchema = bigTableSchema;
    }

    public List<BigTableRecord> convert(List<OdpfMessage> messages) {
        ArrayList<BigTableRecord> records = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            OdpfMessage message = messages.get(index);
            BigTableRecord record = createRecord(message, index);
            records.add(record);
        }
        return records;
    }

    private BigTableRecord createRecord(OdpfMessage message, long index) {
        try {
            ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, modeAndSchema.getFirst(), modeAndSchema.getSecond());
            String rowKey = bigTableRowKeyParser.parse(parsedOdpfMessage);
            RowMutationEntry rowMutationEntry = RowMutationEntry.create(rowKey);
            bigTableSchema.getColumnFamilies().forEach(
                    columnFamily -> bigTableSchema
                            .getColumns(columnFamily)
                            .forEach(column -> {
                                String fieldName = bigTableSchema.getField(columnFamily, column);
                                String value = String.valueOf(parsedOdpfMessage.getFieldByName(fieldName, schema));
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
