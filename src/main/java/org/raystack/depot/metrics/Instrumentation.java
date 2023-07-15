package org.raystack.depot.metrics;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;

/**
 * Instrumentation.
 * <p>
 * Handle logging and metric capturing.
 */
public class Instrumentation implements Closeable {
    @Getter
    private final StatsDReporter statsDReporter;
    @Getter
    private final Logger logger;

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param logger         the logger
     */
    public Instrumentation(StatsDReporter statsDReporter, Logger logger) {
        this.statsDReporter = statsDReporter;
        this.logger = logger;
    }

    /**
     * Instantiates a new Instrumentation.
     *
     * @param statsDReporter the stats d reporter
     * @param clazz          the clazz
     */
    public Instrumentation(StatsDReporter statsDReporter, Class clazz) {
        this.statsDReporter = statsDReporter;
        this.logger = LoggerFactory.getLogger(clazz);
    }

    // =================== LOGGING ===================

    public void logInfo(String template, Object... t) {
        logger.info(template, t);
    }

    public void logWarn(String template, Object... t) {
        logger.warn(template, t);
    }

    public void logDebug(String template, Object... t) {
        logger.debug(template, t);
    }

    public void logError(String template, Object... t) {
        logger.error(template, t);
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    // ===================== CountTelemetry =================

    public void captureCount(String metric, Long count, String... tags) {
        statsDReporter.captureCount(metric, count, tags);
    }

    public void captureHistogram(String metric, Long count, String... tags) {
        statsDReporter.captureHistogram(metric, count, tags);
    }

    public void incrementCounter(String metric, String... tags) {
        statsDReporter.increment(metric, tags);
    }

    public void captureValue(String metric, Integer value, String... tags) {
        statsDReporter.gauge(metric, value, tags);
    }

    public void captureDurationSince(String metric, Instant instant, String... tags) {
        statsDReporter.captureDurationSince(metric, instant, tags);
    }

    public void captureDuration(String metric, long duration, String... tags) {
        statsDReporter.captureDuration(metric, duration, tags);
    }

    // =================== ERROR ===================

    public void captureNonFatalError(String metric, Throwable e, String template, Object... t) {
        logger.warn(template, t);
        logger.warn(e.getMessage(), e);
        statsDReporter.recordEvent(metric, SinkMetrics.NON_FATAL_ERROR, errorTag(e, SinkMetrics.NON_FATAL_ERROR));
    }

    public void captureFatalError(String metric, Throwable e, String template, Object... t) {
        logger.error(template, t);
        logger.error(e.getMessage(), e);
        statsDReporter.recordEvent(metric, SinkMetrics.FATAL_ERROR, errorTag(e, SinkMetrics.FATAL_ERROR));
    }

    private String errorTag(Throwable e, String errorType) {
        return SinkMetrics.ERROR_MESSAGE_CLASS_TAG + "=" + e.getClass().getName() + ",type=" + errorType;
    }
    // ===================== closing =================

    public void close() throws IOException {
        statsDReporter.close();
    }
}
