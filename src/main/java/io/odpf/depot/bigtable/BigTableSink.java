package io.odpf.depot.bigtable;


import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.exception.OdpfSinkException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;

import java.io.IOException;
import java.util.List;

public class BigTableSink implements OdpfSink {
    private static final String COLUMN_FAMILY_NAME = "family-test";
    private final BigtableDataClient dataClient;
    private final BigTableSinkConfig config;
    private final OdpfMessageParser odpfMessageParser;

    public BigTableSink(BigtableDataClient dataClient, BigTableSinkConfig config, OdpfMessageParser odpfMessageParser) {
        this.dataClient = dataClient;
        this.config = config;
        this.odpfMessageParser = odpfMessageParser;
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException {
        OdpfSinkResponse response = new OdpfSinkResponse();
        SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
        String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();

        try {
            BulkMutation batch = BulkMutation.create(config.getTableId());

            for (int i = 0; i < messages.size(); i++) {
                OdpfMessage message = messages.get(i);
                ParsedOdpfMessage parsedOdpfMessage =
                        odpfMessageParser.parse(
                                message,
                                mode,
                                schemaClass);
                batch.add("key#" + i, Mutation.create().setCell(COLUMN_FAMILY_NAME, "odpf_message", parsedOdpfMessage.toString()));
            }

            dataClient.bulkMutateRows(batch);

            System.out.println("Successfully wrote " + messages.size() + "rows");
        } catch (IOException e) {
            System.out.println("Error during WriteBatch: \n" + e);
        }
        return response;
    }

    @Override
    public void close() throws IOException {
    }
}
