package io.odpf.sink.connectors.bigquery.converter;

import lombok.Data;

@Data
public class MessageRecordConverterCache {
    // TODO: Have JsonMessageRecordConverter and ProtoMessageRecordConvert
    private MessageRecordConverter messageRecordConverter;
}
