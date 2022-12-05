package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
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
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
            return new HttpRequestRecord((long) index, null, true, request);
        } catch (IOException e) {
            return createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata());
        } catch (IllegalArgumentException | URISyntaxException e) {
            return createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata());
        }
    }

    private StringEntity buildEntity(String stringBody) {
        return new StringEntity(stringBody, ContentType.APPLICATION_JSON);
    }

    private HttpRequestRecord createErrorRecord(Exception e, ErrorType type, int index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        HttpRequestRecord record = new HttpRequestRecord((long) index, errorInfo, false, null);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        return record;
    }
}
