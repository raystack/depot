package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.exception.InvalidMessageException;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.OdpfMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
public class SingleRequest implements Request {

    private final HttpRequestMethodType httpMethod;
    private final HeaderBuilder headerBuilder;
    private final UriBuilder uriBuilder;
    private final RequestBody requestBody;

    public SingleRequest(HttpRequestMethodType httpMethod, HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestBody requestBody) {
        this.httpMethod = httpMethod;
        this.headerBuilder = headerBuilder;
        this.uriBuilder = uriBuilder;
        this.requestBody = requestBody;
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
            Map<String, String> requestHeaders = headerBuilder.build();
            URI requestUrl = uriBuilder.build(Collections.emptyMap());
            HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(requestUrl, httpMethod);
            requestHeaders.forEach(request::addHeader);
            request.setEntity(buildEntity(requestBody.build(message)));
            return new HttpRequestRecord(Collections.singletonList(index), null, true, request);
        } catch (InvalidMessageException e) {
            return createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, Collections.singletonList(index), message.getMetadata());
        } catch (DeserializerException e) {
            return createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, Collections.singletonList(index), message.getMetadata());
        }
    }

    private HttpRequestRecord createErrorRecord(Exception e, ErrorType type, List<Integer> indexList, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        return new HttpRequestRecord(indexList, errorInfo, false, null);
    }
}