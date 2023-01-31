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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BatchRequest implements Request {

    private final HttpRequestMethodType requestMethod;
    private final Map<String, String> requestHeaders;
    private final URI requestUrl;
    private final RequestBody requestBody;
    private final OdpfMessageParser parser;

    public BatchRequest(HttpRequestMethodType requestMethod,
                        HeaderBuilder headerBuilder,
                        QueryParamBuilder queryParamBuilder,
                        UriBuilder uriBuilder,
                        RequestBody requestBody,
                        OdpfMessageParser parser) {
        this.requestMethod = requestMethod;
        this.requestHeaders = headerBuilder.build();
        this.requestUrl = uriBuilder.build(queryParamBuilder.build());
        this.requestBody = requestBody;
        this.parser = parser;
    }

    @Override
    public List<HttpRequestRecord> createRecords(List<OdpfMessage> messages) {
        ArrayList<HttpRequestRecord> records = new ArrayList<>();
        Map<Integer, String> validBodies = new HashMap<>();
        for (int index = 0; index < messages.size(); index++) {
            OdpfMessage message = messages.get(index);
            MessageContainer messageContainer = new MessageContainer(message, parser);
            try {
                String body = requestBody.build(messageContainer);
                validBodies.put(index, body);
            } catch (EmptyMessageException e) {
                records.add(RequestUtils.createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata()));
            } catch (ConfigurationException | IllegalArgumentException e) {
                records.add(RequestUtils.createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata()));
            } catch (DeserializerException | IOException e) {
                records.add(RequestUtils.createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata()));
            }
        }
        if (validBodies.size() != 0) {
            HttpEntityEnclosingRequestBase request = RequestUtils.buildRequest(
                    requestMethod, requestHeaders, requestUrl, validBodies.values());
            HttpRequestRecord validRecord = new HttpRequestRecord(request);
            validRecord.addAllIndexes(validBodies.keySet());
            records.add(validRecord);
        }
        return records;
    }
}
