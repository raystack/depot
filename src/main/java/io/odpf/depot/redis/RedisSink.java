package io.odpf.depot.redis;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.exception.OdpfSinkException;
import io.odpf.depot.message.OdpfMessage;

import java.io.IOException;
import java.util.List;

public class RedisSink implements OdpfSink {
    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
