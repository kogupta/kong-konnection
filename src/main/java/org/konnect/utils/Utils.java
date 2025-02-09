package org.konnect.utils;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class Utils {
    private Utils() {}

    // extracted from guava stopwatch
    public static String formatTime(long nanos) {
        TimeUnit unit = chooseUnit(nanos);
        double value = (double) nanos / NANOSECONDS.convert(1, unit);
        String s = String.format(Locale.ROOT, "%.4g", value);
        return s + " " + abbreviate(unit);
    }

    private static TimeUnit chooseUnit(long nanos) {
        if (MINUTES.convert(nanos, NANOSECONDS) > 0) return MINUTES;
        if (SECONDS.convert(nanos, NANOSECONDS) > 0) return SECONDS;
        if (MILLISECONDS.convert(nanos, NANOSECONDS) > 0) return MILLISECONDS;
        if (MICROSECONDS.convert(nanos, NANOSECONDS) > 0) return MICROSECONDS;
        return NANOSECONDS;
    }

    private static String abbreviate(TimeUnit unit) {
        return switch (unit) {
            case NANOSECONDS -> "ns";
            case MICROSECONDS -> "μs";
            case MILLISECONDS -> "ms";
            case SECONDS -> "s";
            case MINUTES -> "min";
            case HOURS -> "h";
            case DAYS -> "d";
        };
    }

}
