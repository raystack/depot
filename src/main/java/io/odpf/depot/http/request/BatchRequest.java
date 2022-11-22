package io.odpf.depot.http.request;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.http.enums.HttpRequestMethodType;
import io.odpf.depot.http.record.HttpRequestRecord;
import io.odpf.depot.http.record.HttpRequestRecordUtil;
import io.odpf.depot.http.request.body.RequestBody;
import io.odpf.depot.http.request.builder.HeaderBuilder;
import io.odpf.depot.http.request.builder.UriBuilder;
import io.odpf.depot.message.OdpfMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        HttpRequestRecordUtil recordsUtil = createBody(messages);
        List<Integer> validRecordsIndex = new ArrayList<>(recordsUtil.getValidPayloads().keySet());
        List<String> validRecordsBody = new ArrayList<>(recordsUtil.getValidPayloads().values());
        List<HttpRequestRecord> records = recordsUtil.getRequestRecord();
        if (validRecordsBody.size() != 0) {
            try {
                Map<String, String> requestHeaders = headerBuilder.build();
                URI requestUrl = uriBuilder.build(Collections.emptyMap());
                HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(requestUrl, httpMethod);
                requestHeaders.forEach(request::addHeader);
                request.setEntity(buildEntity(validRecordsBody.toString()));
                records.add(new HttpRequestRecord(validRecordsIndex, null, true, request));
            } catch (URISyntaxException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                List<String> metadata = validRecordsIndex.stream().map(index -> messages.get(index).getMetadataString()).collect(Collectors.toList());
                log.error("Error while parsing record for messages. Metadata : {}, Error: {}", metadata, errorInfo);
                records.add(new HttpRequestRecord(validRecordsIndex, errorInfo, false, null));
            }
        }
        return records;
    }

    private HttpRequestRecordUtil createBody(List<OdpfMessage> messages) {
        Map<Integer, String> validBodies = new HashMap<>();
        List<HttpRequestRecord> failedRecords = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            try {
                String body = requestBody.build(messages.get(index));
                validBodies.put(index, body);
            } catch (ClassCastException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                log.error("Error while creating request body. Metadata : {}, Error: {}", messages.get(index).getMetadataString(), errorInfo);
                failedRecords.add(new HttpRequestRecord(Collections.singletonList(index), errorInfo, false, null));
            }
        }
        return new HttpRequestRecordUtil(validBodies, failedRecords);
    }
}
