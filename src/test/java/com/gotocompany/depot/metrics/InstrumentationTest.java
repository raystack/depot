package com.gotocompany.depot.metrics;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class InstrumentationTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private Logger logger;

    private Instrumentation instrumentation;
    private String testMessage;
    private String testTemplate;
    private Exception e;

    @Before
    public void setUp() {
        instrumentation = new Instrumentation(statsDReporter, logger);
        testMessage = "test";
        testTemplate = "test: {},{},{}";
        e = new Exception();
    }

    @Test
    public void shouldLogString() {
        instrumentation.logInfo(testMessage);
        verify(logger, times(1)).info(testMessage, new Object[]{});
    }

    @Test
    public void shouldLogStringTemplate() {
        instrumentation.logInfo(testTemplate, 1, 2, 3);
        verify(logger, times(1)).info(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogWarnStringTemplate() {
        instrumentation.logWarn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogDebugStringTemplate() {
        instrumentation.logDebug(testTemplate, 1, 2, 3);
        verify(logger, times(1)).debug(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldLogErrorStringTemplate() {
        instrumentation.logError(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringMessage() {
        instrumentation.captureNonFatalError("test_metric", e, testMessage);
        verify(logger, times(1)).warn(testMessage, new Object[]{});
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test_metric", SinkMetrics.NON_FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.NON_FATAL_ERROR);
    }

    @Test
    public void shouldCaptureNonFatalErrorWithStringTemplate() {
        instrumentation.captureNonFatalError("test_metric", e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(testTemplate, 1, 2, 3);
        verify(logger, times(1)).warn(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test_metric", SinkMetrics.NON_FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.NON_FATAL_ERROR);
    }

    @Test
    public void shouldCaptureFatalErrorWithStringMessage() {
        instrumentation.captureFatalError("test_metric", e, testMessage);
        verify(logger, times(1)).error(testMessage, new Object[]{});
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test_metric", SinkMetrics.FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.FATAL_ERROR);
    }

    @Test
    public void shouldCaptureFatalErrorWithStringTemplate() {
        instrumentation.captureFatalError("test", e, testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(testTemplate, 1, 2, 3);
        verify(logger, times(1)).error(e.getMessage(), e);
        verify(statsDReporter, times(1)).recordEvent("test", SinkMetrics.FATAL_ERROR, SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + SinkMetrics.FATAL_ERROR);
    }

    @Test
    public void shouldCaptureCountWithTags() {
        String metric = "test_metric";
        String urlTag = "url=test";
        String httpCodeTag = "status_code=200";
        instrumentation.captureCount(metric, 1L, httpCodeTag, urlTag);
        verify(statsDReporter, times(1)).captureCount(metric, 1L, httpCodeTag, urlTag);
    }

    @Test
    public void shouldIncrementCounterWithTags() {
        String metric = "test_metric";
        String httpCodeTag = "status_code=200";
        instrumentation.incrementCounter(metric, httpCodeTag);
        verify(statsDReporter, times(1)).increment(metric, httpCodeTag);
    }

    @Test
    public void shouldIncrementCounter() {
        String metric = "test_metric";
        instrumentation.incrementCounter(metric);
        verify(statsDReporter, times(1)).increment(metric, new String[]{});
    }

    @Test
    public void shouldClose() throws IOException {
        instrumentation.close();
        verify(statsDReporter, times(1)).close();
    }
}
