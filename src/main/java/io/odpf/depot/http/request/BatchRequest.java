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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
public class BatchRequest implements Request {

    private final HttpRequestMethodType httpMethod;
    private final HeaderBuilder headerBuilder;
    private final UriBuilder uriBuilder;
    private final RequestBody requestBody;

    public BatchRequest(HttpRequestMethodType httpMethod, HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestBody requestBody) {
        this.httpMethod = httpMethod;
        this.headerBuilder = headerBuilder;
        this.uriBuilder = uriBuilder;
        this.requestBody = requestBody;
    }

    @Override
    public List<HttpRequestRecord> createRecords(List<OdpfMessage> messages) {
        List<String> payloads = new ArrayList<>();
        HttpRequestRecord record;
        try {
            IntStream.range(0, messages.size()).forEach(index -> {
                String body = requestBody.build(messages.get(index));
                if (body != null) {
                    payloads.add(body);
                }
            });
            Map<String, String> requestHeaders = headerBuilder.build();
            URI requestUrl = uriBuilder.build(Collections.emptyMap());
            HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(requestUrl, httpMethod);
            requestHeaders.forEach(request::addHeader);
            request.setEntity(buildEntity(payloads.toString()));
            record = new HttpRequestRecord((long) 0, null, true, request);
            return Collections.singletonList(record);
        } catch (Exception e) {
            Map<String, Object> metadata = messages.get(0).getMetadata();
            ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DEFAULT_ERROR);
            log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
            record = createAndLogErrorRecord(errorInfo, 0);
            return Collections.singletonList(record);
        }
    }
}
