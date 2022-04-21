package io.odpf.sink.connectors;

import io.odpf.sink.connectors.expcetion.OdpfSinkException;
import io.odpf.sink.connectors.message.OdpfMessage;

import java.io.Closeable;
import java.util.List;

public interface OdpfSink extends Closeable {

    OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException;
}


