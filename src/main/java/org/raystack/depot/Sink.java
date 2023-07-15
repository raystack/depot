package org.raystack.depot;

import org.raystack.depot.exception.SinkException;
import org.raystack.depot.message.Message;

import java.io.Closeable;
import java.util.List;

public interface Sink extends Closeable {

    SinkResponse pushToSink(List<Message> messages) throws SinkException;
}
