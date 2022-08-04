package io.odpf.depot.redis.parsers;


import com.google.gson.Gson;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.models.RedisRecord;
import io.odpf.depot.redis.models.RedisRecords;
import lombok.AllArgsConstructor;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {
    private OdpfMessageParser odpfMessageParser;

    private RedisSinkConfig redisSinkConfig;

    public abstract List<RedisDataEntry> parse(OdpfMessage message);

    String parseKeyTemplate(String template, Map<String, Object> columns) {
        String key = "";
        String field = "";
        // example template: "ID_{order_number}_URL_{order_url}"
        for (int i = 0; i < template.length(); i++) {
            char c = template.charAt(i);
            if (c == '{') {
                for (int ii = i + 1; ii < template.length(); ii++) {
                    char f = template.charAt(ii);
                    if (f == '}') {
                        i = ii;
                        break;
                    }
                    field += f;
                }
                JSONObject jObject = new JSONObject(new Gson().toJson(columns));

                Configuration configuration = Configuration.builder()
                        .jsonProvider(new JsonOrgJsonProvider())
                        .build();

                JsonPath jsonPath = JsonPath.compile(field);
                Object jsonPathString = jsonPath.read(jObject, configuration);
                key += jsonPathString;
                field = "";
            } else {
                key += c;
            }

        }
        return key;
    }

    public RedisRecords convert(List<OdpfMessage> messages) {
        List<RedisRecord> valid = new ArrayList<>();
        List<RedisRecord> invalid = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            try {
                List<RedisDataEntry> p = parse(messages.get(i));
                for (int ii = 0; ii < p.size(); ii++) {
                    valid.add(new RedisRecord(p.get(ii), (long) valid.size(), new ErrorInfo(null, null)));
                }
            } catch (Exception e) {
                // generic handler
                invalid.add(new RedisRecord(null, (long) i, new ErrorInfo(e, ErrorType.DEFAULT_ERROR)));
            }
        }
        return new RedisRecords(valid, invalid);
    }
}
