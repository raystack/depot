package com.gotocompany.depot;

import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;

import java.io.Closeable;
import java.util.List;

public interface Sink extends Closeable {

    SinkResponse pushToSink(List<Message> messages) throws SinkException;
}


