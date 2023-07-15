package org.raystack.depot;

import org.raystack.depot.exception.OdpfSinkException;
import org.raystack.depot.message.OdpfMessage;

import java.io.Closeable;
import java.util.List;

public interface OdpfSink extends Closeable {

    OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException;
}
