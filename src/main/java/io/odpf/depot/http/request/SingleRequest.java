package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.MessageContainer;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
public class SingleRequest implements Request {

    private final HttpRequestMethodType requestMethodType;
    private final HeaderBuilder headerBuilder;
    private final UriBuilder uriBuilder;
    private final RequestBody requestBody;
    private final OdpfMessageParser odpfMessageParser;

    public SingleRequest(HttpRequestMethodType requestMethodType, HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestBody requestBody, OdpfMessageParser odpfMessageParser) {
        this.requestMethodType = requestMethodType;
        this.headerBuilder = headerBuilder;
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
            Map<String, String> requestHeaders = headerBuilder.build(new MessageContainer(message), odpfMessageParser);
            URI requestUrl = uriBuilder.build(Collections.emptyMap());
            HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(requestUrl, requestMethodType);
            requestHeaders.forEach(request::addHeader);
            request.setEntity(buildEntity(requestBody.build(message)));
            HttpRequestRecord record = new HttpRequestRecord(null, true, request);
            record.addIndex(index);
            return record;
        } catch (EmptyMessageException e) {
            return createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata());
        } catch (ConfigurationException | IllegalArgumentException e) {
            return createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata());
        } catch (DeserializerException | IOException e) {
            return createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata());
        }
    }

    private HttpRequestRecord createErrorRecord(Exception e, ErrorType type, Integer index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        HttpRequestRecord record = new HttpRequestRecord(errorInfo, false, null);
        record.addIndex(index);
        return record;
    }
}