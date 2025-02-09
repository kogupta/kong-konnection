package org.konnect.utils;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

public final class Resources {
    private Resources() {}

    public static String readFromClasspath(String name) {
        ClassLoader loader = Resources.class.getClassLoader();
        URL url = loader.getResource(name);
        try (InputStream is = Objects.requireNonNull(url).openStream()) {
            byte[] bytes = is.readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path fileInProjectRoot(String... s) {
        String root = System.getProperty("user.dir");
        return Paths.get(root, s);
    }

    public static void main() throws IOException {
        testReadingClasspathFile();
        testReadingProjectDirFile();
    }

    private static void testReadingProjectDirFile() throws IOException {
        Path path = fileInProjectRoot("stream.jsonl");
        try (Stream<String> lines = Files.lines(path)) {
            int rows = lines.mapToInt(_ -> 1).sum();
            assertThat(rows == 809, "809 rows expected, got:" + rows);
        }
    }

    private static void testReadingClasspathFile() throws IOException {
        String s = readFromClasspath("kafkaWriter.properties");
        Properties props = new Properties();
        props.load(new CharArrayReader(s.toCharArray()));
        assertThat(props.getProperty("bootstrap.servers"), "localhost:9092");
        assertThat(props.getProperty("compression.type"), "lz4");
        assertThat(props.getProperty("topic"), "cdc-events");
    }

    private static void assertThat(boolean predicate, String msg) {
        if (!predicate)
            throw new AssertionError(msg);
    }

    private static <T> void assertThat(T obtained, T expected) {
        if (!Objects.equals(expected, obtained)) {
            System.out.println("Obtained: " + obtained);
            System.out.println("Expected: " + expected);

            String s = """
                Expected: %s
                got:      %s""".formatted(expected, obtained);
            throw new AssertionError(s);
        }
    }
}
