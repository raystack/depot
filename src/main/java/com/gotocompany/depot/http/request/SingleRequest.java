package com.gotocompany.depot.http.request;

import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import com.gotocompany.depot.http.request.body.RequestBody;
import com.gotocompany.depot.http.request.builder.HeaderBuilder;
import com.gotocompany.depot.http.request.builder.QueryParamBuilder;
import com.gotocompany.depot.http.request.builder.UriBuilder;
import com.gotocompany.depot.message.MessageContainer;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
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
    private final MessageParser messageParser;

    public SingleRequest(HttpRequestMethodType requestMethod,
                         HeaderBuilder headerBuilder,
                         QueryParamBuilder queryParamBuilder,
                         UriBuilder uriBuilder,
                         RequestBody requestBody,
                         MessageParser messageParser) {
        this.requestMethod = requestMethod;
        this.headerBuilder = headerBuilder;
        this.queryParamBuilder = queryParamBuilder;
        this.uriBuilder = uriBuilder;
        this.requestBody = requestBody;
        this.messageParser = messageParser;
    }

    @Override
    public List<HttpRequestRecord> createRecords(List<Message> messages) {
        ArrayList<HttpRequestRecord> records = new ArrayList<>();
        IntStream.range(0, messages.size()).forEach(index -> {
            Message message = messages.get(index);
            HttpRequestRecord record = createRecord(message, index);
            records.add(record);
        });
        return records;
    }

    private HttpRequestRecord createRecord(Message message, int index) {
        try {
            MessageContainer messageContainer = new MessageContainer(message, messageParser);
            Map<String, String> requestHeaders = headerBuilder.build(messageContainer);
            Map<String, String> queryParam = queryParamBuilder.build(messageContainer);
            URI requestUrl = uriBuilder.build(messageContainer, queryParam);
            HttpEntityEnclosingRequestBase request = RequestUtils.buildRequest(
                    requestMethod, requestHeaders, requestUrl, requestBody.build(messageContainer));
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
