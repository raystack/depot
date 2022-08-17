package io.odpf.depot.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.metrics.StatsDReporter;

import java.io.IOException;

public class BigTableSinkFactory {
    private BigtableDataClient bigtableDataClient;
    private OdpfMessageParser odpfMessageParser;
    private final BigTableSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;

    public BigTableSinkFactory(BigTableSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public void init() {
        try {
            odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
            bigtableDataClient = BigtableDataClient.create(sinkConfig.getGCloudProjectID(), sinkConfig.getBigtableInstanceId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public OdpfSink create() {
        return new BigTableSink(
                bigtableDataClient,
                sinkConfig,
                odpfMessageParser);
    }
}
