package org.raystack.depot;

import org.raystack.depot.exception.RaystackSinkException;
import org.raystack.depot.message.RaystackMessage;

import java.io.Closeable;
import java.util.List;

public interface RaystackSink extends Closeable {

    RaystackSinkResponse pushToSink(List<RaystackMessage> messages) throws RaystackSinkException;
}
