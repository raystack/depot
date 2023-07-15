package org.raystack.depot.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {
    private static final TimeZone TZ = TimeZone.getTimeZone("UTC");
    private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    static {
        DF.setTimeZone(TZ);
    }

    public static String formatCurrentTimeAsUTC() {
        return formatTimeAsUTC(new Date());
    }

    public static String formatTimeAsUTC(Date date) {
        return DF.format(date);
    }
}
