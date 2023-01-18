package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.QueryParamBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
public class SingleRequest implements Request {

    private final HttpRequestMethodType requestMethod;
    private final HeaderBuilder headerBuilder;
    private final QueryParamBuilder queryParamBuilder;
    private final UriBuilder uriBuilder;
    private final RequestBody requestBody;
    private final OdpfMessageParser odpfMessageParser;

    public SingleRequest(HttpRequestMethodType requestMethod,
                         HeaderBuilder headerBuilder,
                         QueryParamBuilder queryParamBuilder,
                         UriBuilder uriBuilder,
                         RequestBody requestBody,
                         OdpfMessageParser odpfMessageParser) {
        this.requestMethod = requestMethod;
        this.headerBuilder = headerBuilder;
        this.queryParamBuilder = queryParamBuilder;
        this.uriBuilder = uriBuilder;
        this.requestBody = requestBody;
        this.odpfMessageParser = odpfMessageParser;
    }

    @Override
    public List<HttpRequestRecord> createRecords(List<OdpfMessage> messages) {
        ArrayList<HttpRequestRecord> records = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            OdpfMessage message = messages.get(index);
            HttpRequestRecord record = createRecord(message, index);
            records.add(record);
        });
        return records;
    }

    private HttpRequestRecord createRecord(OdpfMessage message, int index) {
        try {
            MessageContainer messageContainer = new MessageContainer(message, odpfMessageParser);
            Map<String, String> requestHeaders = headerBuilder.build(messageContainer);
            Map<String, String> queryParam = queryParamBuilder.build(messageContainer);
            URI requestUrl = uriBuilder.build(messageContainer, queryParam);
            HttpEntityEnclosingRequestBase request = RequestUtils.buildRequest(requestMethod, requestHeaders, requestUrl, requestBody.build(message));
            HttpRequestRecord record = new HttpRequestRecord(request);
            record.addIndex(index);
            return record;
        } catch (EmptyMessageException e) {
            return RequestUtils.createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata());
        } catch (ConfigurationException | IllegalArgumentException e) {
            return RequestUtils.createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata());
        } catch (DeserializerException | IOException e) {
            return RequestUtils.createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata());
        }
    }
}
