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
import java.util.List;
import java.util.Map;

@Slf4j
public class SingleRequest implements Request {

    private final HttpRequestMethodType httpMethod;
    private final HeaderBuilder headerBuilder;
    private final UriBuilder uriBuilder;
    private final RequestBody requestBody;
//    private final OdpfMessageSchema keySchema;
//    private final OdpfMessageSchema valueSchema;

    public SingleRequest(HttpRequestMethodType httpMethod, HeaderBuilder headerBuilder, UriBuilder uriBuilder, RequestBody requestBody) {
        this.httpMethod = httpMethod;
        this.headerBuilder = headerBuilder;
        this.uriBuilder = uriBuilder;
        this.requestBody = requestBody;
//        this.keySchema = keySchema;
//        this.valueSchema = valueSchema;
    }

    @Override
    public List<HttpRequestRecord> createRecords(List<OdpfMessage> messages) {
        List<HttpRequestRecord> records = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            try {
                Map<String, String> requestHeaders = headerBuilder.build();
                URI requestUrl = uriBuilder.build();

//            MessageContainer messageContainer = new MessageContainer(message);
//            Map<String, String> requestHeaders = config.getHeaderSourceMode() == KEY ?
//                    headerBuilder.build(messageContainer.getKeyParsedMessage(), keySchema)
//                    : headerBuilder.build(messageContainer.getValueParsedMessage(), valueSchema);

//            Map<String, String> queryParam = config.getQueryParamSourceMode() == KEY ?
//                    queryParamBuilder.build(messageContainer.getKeyParsedMessage(), keySchema)
//                    : queryParamBuilder.build(messageContainer.getValueParsedMessage(), valueSchema);

//            URI requestUrl = config.getURLSourceMode() == KEY ? uriBuilder.build(messageContainer.getKeyParsedMessage(), keySchema, queryParam)
//                    : uriBuilder.build(messageContainer.getValueParsedMessage(), valueSchema, queryParam);
//
                HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(requestUrl, httpMethod);
                requestHeaders.forEach(request::addHeader);
                request.setEntity(buildEntity(requestBody.build(messages.get(index))));

                records.add(new HttpRequestRecord(request, (long) index, null, true));
            } catch (Exception e) {
                records.add(createAndLogErrorRecord(e, ErrorType.DEFAULT_ERROR, index));
            }
        }
        return records;
    }

    private StringEntity buildEntity(String stringBody) {
        return new StringEntity(stringBody, ContentType.APPLICATION_JSON);
    }

    private HttpRequestRecord createAndLogErrorRecord(Exception e, ErrorType type, int index) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        HttpRequestRecord record = new HttpRequestRecord(null, (long) index, errorInfo, false);
        log.error("Error while parsing record for message. Record: {}, Error: {}", record.getRequestBody(), errorInfo);
        return record;
    }
}
