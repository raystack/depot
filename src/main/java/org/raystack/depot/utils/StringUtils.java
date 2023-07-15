package org.raystack.depot.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class StringUtils {

    private static final Pattern PATTERN = Pattern.compile("(?!<%)%"
            + "(?:(\\d+)\\$)?"
            + "([-#+ 0,(]|<)?"
            + "\\d*"
            + "(?:\\.\\d+)?"
            + "(?:[bBhHsScCdoxXeEfgGaAtT]|"
            + "[tT][HIklMSLNpzZsQBbhAaCYyjmdeRTrDFc])");

    public static int countVariables(String fmt) {
        Matcher m = PATTERN.matcher(fmt);
        int np = 0;
        int maxref = 0;
        while (m.find()) {
            if (m.group(1) != null) {
                String dec = m.group(1);
                int ref = Integer.parseInt(dec);
                maxref = Math.max(ref, maxref);
            } else if (!(m.group(2) != null && "<".equals(m.group(2)))) {
                np++;
            }
        }
        return Math.max(np, maxref);
    }

    public static int count(String in, char c) {
        return IntStream.range(0, in.length()).reduce(0, (x, y) -> x + (in.charAt(y) == c ? 1 : 0));
    }
}
