package com.gotocompany.depot.bigquery.storage.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.BigQuerySinkConfig;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class TimeStampUtils {
    private static final long FIVE_YEARS_DAYS = 1825;
    private static final long ONE_YEAR_DAYS = 365;
    private static final Instant MIN_TIMESTAMP = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant MAX_TIMESTAMP = Instant.parse("9999-12-31T23:59:59.999999Z");

    public static long getBQInstant(Instant instant, Descriptors.FieldDescriptor fieldDescriptor, boolean isTopLevel, BigQuerySinkConfig config) {
        // Timestamp should be in microseconds
        long timeStamp = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
        // Partition column is always top level
        if (isTopLevel && fieldDescriptor.getName().equals(config.getTablePartitionKey())) {
            Instant currentInstant = Instant.now();
            boolean isValid;
            boolean isPastInstant = currentInstant.isAfter(instant);
            if (isPastInstant) {
                Instant fiveYearPast = currentInstant.minusMillis(TimeUnit.DAYS.toMillis(FIVE_YEARS_DAYS));
                isValid = fiveYearPast.isBefore(instant);
            } else {
                Instant oneYearFuture = currentInstant.plusMillis(TimeUnit.DAYS.toMillis(ONE_YEAR_DAYS));
                isValid = oneYearFuture.isAfter(instant);

            }
            if (!isValid) {
                throw new UnsupportedOperationException(instant + " for field "
                        + fieldDescriptor.getFullName() + " is outside the allowed bounds. "
                        + "You can only stream to date range within 1825 days in the past "
                        + "and 366 days in the future relative to the current date.");
            }
            return timeStamp;
        } else {
            // other timestamps should be in the limit specifies by BQ
            if (instant.isAfter(MIN_TIMESTAMP) && instant.isBefore(MAX_TIMESTAMP)) {
                return timeStamp;
            } else {
                throw new UnsupportedOperationException(instant
                        + " for field "
                        + fieldDescriptor.getFullName()
                        + " is outside the allowed bounds in BQ.");
            }
        }
    }
}
