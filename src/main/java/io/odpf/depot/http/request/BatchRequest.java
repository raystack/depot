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
import io.odpf.depot.message.OdpfMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        ArrayList<HttpRequestRecord> records = new ArrayList<>();
        Map<Integer, String> validBodies = new HashMap<>();
        for (int index = 0; index < messages.size(); index++) {
            OdpfMessage message = messages.get(index);
            try {
                String body = requestBody.build(messages.get(index));
                validBodies.put(index, body);
            } catch (EmptyMessageException e) {
                records.add(createErrorRecord(e, ErrorType.INVALID_MESSAGE_ERROR, index, message.getMetadata()));
            } catch (ConfigurationException | IllegalArgumentException e) {
                records.add(createErrorRecord(e, ErrorType.UNKNOWN_FIELDS_ERROR, index, message.getMetadata()));
            } catch (DeserializerException | IOException e) {
                records.add(createErrorRecord(e, ErrorType.DESERIALIZATION_ERROR, index, message.getMetadata()));
            }
        }
        List<Integer> validRecordsIndex = new ArrayList<>(validBodies.keySet());
        List<String> validRecordsBody = new ArrayList<>(validBodies.values());
        if (validRecordsBody.size() != 0) {
            Map<String, String> requestHeaders = headerBuilder.build();
            URI requestUrl = uriBuilder.build(Collections.emptyMap());
            HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(requestUrl, httpMethod);
            requestHeaders.forEach(request::addHeader);
            request.setEntity(buildEntity(validRecordsBody.toString()));
            HttpRequestRecord validRecord = new HttpRequestRecord(null, true, request);
            validRecordsIndex.forEach(validRecord::addIndex);
            records.add(validRecord);
        }
        return records;
    }

    private HttpRequestRecord createErrorRecord(Exception e, ErrorType type, Integer index, Map<String, Object> metadata) {
        ErrorInfo errorInfo = new ErrorInfo(e, type);
        log.error("Error while parsing record for message. Metadata : {}, Error: {}", metadata, errorInfo);
        HttpRequestRecord record = new HttpRequestRecord(errorInfo, false, null);
        record.addIndex(index);
        return record;
    }
}
