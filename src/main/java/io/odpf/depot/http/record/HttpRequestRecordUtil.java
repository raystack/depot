package io.odpf.depot.http.record;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Getter
public class HttpRequestRecordUtil {

    private final Map<Integer, String> validPayloads;
    private final List<HttpRequestRecord> requestRecord;
}
