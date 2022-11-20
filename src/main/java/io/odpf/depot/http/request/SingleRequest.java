package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.OdpfMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

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
            Map<String, String> requestHeaders = headerBuilder.build(message);
            URI requestUrl = uriBuilder.build(Collections.emptyMap());
            HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(requestUrl, httpMethod);
            requestHeaders.forEach(request::addHeader);
            request.setEntity(buildEntity(requestBody.build(message)));
            return new HttpRequestRecord((long) index, null, true, request);
        } catch (Exception e) {
            return createAndLogErrorRecord(e, ErrorType.DEFAULT_ERROR, index, message.getMetadata());
        }
    }

    private StringEntity buildEntity(String stringBody) {
        return new StringEntity(stringBody, ContentType.APPLICATION_JSON);
    }

    private HttpRequestRecord createAndLogErrorRecord(Exception e, ErrorType type, int index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        HttpRequestRecord record = new HttpRequestRecord((long) index, errorInfo, false, null);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        return record;
    }
}
