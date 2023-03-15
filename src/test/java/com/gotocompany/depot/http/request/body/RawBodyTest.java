package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.TestMessage;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class RawBodyTest {

    private Message message;
    private MessageContainer messageContainer;
    @Mock
    private MessageParser parser;
    @Mock
    private HttpSinkConfig config;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new Message(testMessage.toByteArray(), testMessage.toByteArray());
        messageContainer = new MessageContainer(message, parser);
    }

    @Test
    public void shouldWrapProtoByteInsideJson() throws IOException {
        RequestBody body = new RawBody(config);
        String rawBody = body.build(messageContainer);
        assertTrue(new JSONObject("{\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}").similar(new JSONObject(rawBody)));
    }

    @Test
    public void shouldPutEmptyStringIfKeyIsNull() throws IOException {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new Message(null, testMessage.toByteArray());
        messageContainer = new MessageContainer(message, parser);
        RequestBody body = new RawBody(config);
        String rawBody = body.build(messageContainer);
        assertTrue(new JSONObject("{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}").similar(new JSONObject(rawBody)));
    }

    @Test
    public void shouldAddMetadataToRawBody() throws IOException {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new Message(
                testMessage.toByteArray(),
                testMessage.toByteArray(),
                new Tuple<>("message_topic", "sample-topic"),
                new Tuple<>("message_partition", 1));
        messageContainer = new MessageContainer(message, parser);
        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_ADD_METADATA_ENABLED", "true");
        configuration.put("SINK_METADATA_COLUMNS_TYPES", "message_partition=integer,message_topic=string");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        RequestBody body = new RawBody(config);
        String rawBody = body.build(messageContainer);
        assertTrue(new JSONObject("{\"message_partition\":1,\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"message_topic\":\"sample-topic\"}").similar(new JSONObject(rawBody)));
    }

    @Test(expected = IOException.class)
    public void shouldThrowExceptionIfMessageIsNotBytes() throws IOException {
        message = new Message("", "test-string");
        messageContainer = new MessageContainer(message, parser);
        RequestBody body = new RawBody(config);
        body.build(messageContainer);
    }
}
