package com.gotocompany.depot.log;

import com.gotocompany.depot.message.json.JsonMessageParser;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LogSinkTest {
    private final String template = "\n================= DATA =======================\n{}\n================= METADATA =======================\n{}\n";
    private SinkConfig config;
    private MessageParser messageParser;
    private Instrumentation instrumentation;
    private JsonParserMetrics jsonParserMetrics;

    @Before
    public void setUp() throws Exception {
        config = mock(SinkConfig.class);
        messageParser = mock(MessageParser.class);
        instrumentation = mock(Instrumentation.class);
        jsonParserMetrics = new JsonParserMetrics(config);

    }

    @Test
    public void shouldProcessEmptyMessageWithNoError() throws IOException {
        LogSink logSink = new LogSink(config, messageParser, instrumentation);
        ArrayList<Message> messages = new ArrayList<>();
        SinkResponse sinkResponse = logSink.pushToSink(messages);
        Map<Long, ErrorInfo> errors = sinkResponse.getErrors();

        assertEquals(Collections.emptyMap(), errors);
        verify(messageParser, never()).parse(any(), any(), any());
        verify(instrumentation, never()).logInfo(any(), any(), any());
    }

    @Test
    public void shouldLogJsonMessages() throws SinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {{
            put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "log_message");
        }};
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);
        messageParser = new JsonMessageParser(sinkConfig, instrumentation, jsonParserMetrics);
        LogSink logSink = new LogSink(sinkConfig, messageParser, instrumentation);
        ArrayList<Message> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String validJsonLastName = "{\"last_name\":\"doe\"}";
        byte[] logMessage2 = validJsonLastName.getBytes();
        messages.add(new Message(null, logMessage1));
        messages.add(new Message(null, logMessage2));
        SinkResponse sinkResponse = logSink.pushToSink(messages);

        //assert no error
        Map<Long, ErrorInfo> errors = sinkResponse.getErrors();
        assertEquals(Collections.emptyMap(), errors);

        //assert processed message
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(2)).logInfo(eq(template), jsonStrCaptor.capture(), eq(Collections.emptyMap().toString()));
        assertThat(jsonStrCaptor.getAllValues(), containsInAnyOrder(validJsonFirstName, validJsonLastName));
    }

    @Test
    public void shouldReturnErrorResponseAndProcessValidMessage() throws SinkException {
        HashMap<String, String> configMap = new HashMap<String, String>() {{
            put("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", "log_message");
        }};
        SinkConfig sinkConfig = ConfigFactory.create(SinkConfig.class, configMap);

        messageParser = new JsonMessageParser(sinkConfig, instrumentation, jsonParserMetrics);
        LogSink logSink = new LogSink(sinkConfig, messageParser, instrumentation);
        ArrayList<Message> messages = new ArrayList<>();
        String validJsonFirstName = "{\"first_name\":\"john\"}";
        byte[] logMessage1 = validJsonFirstName.getBytes();
        String invalidJson = "{\"last_name";
        byte[] invalidLogMessage = invalidJson.getBytes();
        messages.add(new Message(null, logMessage1));
        messages.add(new Message(null, invalidLogMessage));
        SinkResponse sinkResponse = logSink.pushToSink(messages);

        //assert error
        ErrorInfo error = sinkResponse.getErrorsFor(1L);
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, error.getErrorType());

        //assert valid message processed
        ArgumentCaptor<String> jsonStrCaptor = ArgumentCaptor.forClass(String.class);
        verify(instrumentation, times(1)).logInfo(eq(template), jsonStrCaptor.capture(), eq(Collections.emptyMap().toString()));
        assertEquals(validJsonFirstName, jsonStrCaptor.getValue().toString());
    }
}
