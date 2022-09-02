package io.odpf.depot.bigtable.parser;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.odpf.depot.bigtable.client.BigTableClient;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.model.BigTableRowCell;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BigTableRecordParser {
    private final BigTableSinkConfig sinkConfig;
    private final OdpfMessageParser odpfMessageParser;
    private final BigTableRowKeyParser bigTableRowKeyParser;

    public BigTableRecordParser(BigTableSinkConfig sinkConfig, OdpfMessageParser odpfMessageParser, BigTableRowKeyParser bigTableRowKeyParser) {
        this.sinkConfig = sinkConfig;
        this.odpfMessageParser = odpfMessageParser;
        this.bigTableRowKeyParser = bigTableRowKeyParser;
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
        SinkConnectorSchemaMessageMode mode = sinkConfig.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? sinkConfig.getSinkConnectorSchemaProtoMessageClass() : sinkConfig.getSinkConnectorSchemaProtoKeyClass();
        try {
            String rowKey = bigTableRowKeyParser.parse(sinkConfig.getRowKeyTemplate(), message);
            RowMutationEntry rowMutationEntry = RowMutationEntry.create(rowKey);

            ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, mode, schemaClass);
            List<BigTableRowCell> rowCells = createRowCells(parsedOdpfMessage, schemaClass);
            rowCells.forEach(cell -> rowMutationEntry.setCell(cell.getColumnFamily(), cell.getQualifier(), cell.getValue()));

            return new BigTableRecord(rowMutationEntry, index, null, true);
        } catch (Exception e) {
            ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
            return new BigTableRecord(null, index, errorInfo, false);
        }
    }

    private List<BigTableRowCell> createRowCells(ParsedOdpfMessage parsedOdpfMessage, String schemaClass) throws IOException {
        JSONObject inputOutputFieldMapping = new JSONObject(sinkConfig.getSinkConnectorInputOutputFieldMapping());
        List<BigTableRowCell> cells = new ArrayList<>();
        for (String columnFamily : inputOutputFieldMapping.keySet()) {
            JSONObject qualifierInputFieldMap = inputOutputFieldMapping.getJSONObject(columnFamily);

            for (String qualifier : qualifierInputFieldMap.keySet()) {
                cells.add(new BigTableRowCell(columnFamily, qualifier,
                        String.valueOf(parsedOdpfMessage.getFieldByName(
                                qualifierInputFieldMap.getString(qualifier),
                                odpfMessageParser.getSchema(schemaClass)))));

            }
        }
        return cells;
    }
}
