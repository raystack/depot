package io.odpf.depot;

import io.odpf.depot.exception.OdpfSinkException;
import io.odpf.depot.message.OdpfMessage;

import java.io.Closeable;
import java.util.List;

public interface OdpfSink extends Closeable {

    OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException;
}


