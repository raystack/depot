package io.odpf.depot.http.request.body;

import io.odpf.depot.TestMessage;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.InvalidMessageException;
import io.odpf.depot.message.OdpfMessage;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class RawBodyTest {

    private OdpfMessage message;

    @Mock
    private HttpSinkConfig config;

    @Test
    public void shouldWrapProtoByteInsideJson() {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new OdpfMessage(testMessage.toByteArray(), testMessage.toByteArray());
        RequestBody body = new RawBody(config);
        String rawBody = body.build(message);
        assertTrue(new JSONObject("{\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}").similar(new JSONObject(rawBody)));
    }

    @Test
    public void shouldPutEmptyStringIfKeyIsNull() {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new OdpfMessage(null, testMessage.toByteArray());
        RequestBody body = new RawBody(config);
        String rawBody = body.build(message);
        assertEquals("{\"log_key\":\"\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\"}", rawBody);
    }

    @Test
    public void shouldAddMetadataToRawBody() {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order-1").setOrderDetails("ORDER-DETAILS-1").build();
        message = new OdpfMessage(
                testMessage.toByteArray(),
                testMessage.toByteArray(),
                new Tuple<>("message_topic", "sample-topic"),
                new Tuple<>("message_partition", 1));
        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_ADD_METADATA_ENABLED", "true");
        configuration.put("SINK_METADATA_COLUMNS_TYPES", "message_partition=integer,message_topic=string");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);

        RequestBody body = new RawBody(config);
        String rawBody = body.build(message);
        assertTrue(new JSONObject("{\"message_partition\":1,\"log_key\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"log_message\":\"Cgx0ZXN0LW9yZGVyLTEaD09SREVSLURFVEFJTFMtMQ==\",\"message_topic\":\"sample-topic\"}").similar(new JSONObject(rawBody)));
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowExceptionIfMessageIsNotBytes() {
        message = new OdpfMessage("", "test-string");
        RequestBody body = new RawBody(config);
        body.build(message);
    }
}
