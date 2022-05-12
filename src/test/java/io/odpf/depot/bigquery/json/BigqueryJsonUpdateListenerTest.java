package io.odpf.depot.bigquery.json;

import io.odpf.depot.bigquery.handler.MessageRecordConverter;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BigqueryJsonUpdateListenerTest {

    @Test
    public void shouldSetMessageRecordConverter() throws Exception {
        MessageRecordConverterCache converterCache = mock(MessageRecordConverterCache.class);
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(null, converterCache);
        bigqueryJsonUpdateListener.setOdpfMessageParser(null);
        bigqueryJsonUpdateListener.onSchemaUpdate(null);
        verify(converterCache, times(1)).setMessageRecordConverter(any(MessageRecordConverter.class));
    }
}
