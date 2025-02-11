package org.konnect.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

public final class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

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

    public static Properties loadPropsOrExit(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        boolean fileExists = Files.exists(path) && Files.isRegularFile(path);
        if (!fileExists) {
            String s = path.toFile().getAbsolutePath();
            String err = !Files.exists(path) ? "Non-existent file: " + s : "Not a regular file: " + s;
            logger.error(err);
            System.exit(1);
        }

        Properties props = new Properties();
        props.load(Files.newInputStream(path));
        return props;
    }

    public static void uncheckedSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {}
    }
}
