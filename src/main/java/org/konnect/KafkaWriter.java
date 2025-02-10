package org.konnect;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.konnect.utils.Resources;
import org.konnect.utils.Utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class KafkaWriter {
    private static final String inputFileKey = "inputFile";

    private KafkaWriter() {}

    public static void main(String[] args) throws IOException {
        // usage: KafkaWriter $configFile
        Properties props = args.length < 1 ? loadFromClasspath() : Utils.loadPropsOrExit(args[0]);
        start(props);
    }

    private static Properties loadFromClasspath() {
        System.out.println("""
            Usage: KafkaWriter /path/to/config/file.properties
                Reading `resources/kafkaWriter.properties` instead""");
        return Resources.loadFromClasspath("kafkaWriter.properties");
    }

    ///  Start writing to Kafka.
    ///
    /// Parameter `absolutePath` refers to writer properties file - it includes:
    ///   - path to input `*.jsonl` file
    ///   - Kafka producer properties (`linger.ms`, `batch.size`, `acks`, etc.)
    private static void start(Properties properties) throws IOException {
        String inputFile = getInputFile(properties);

        StringSerializer serializer = new StringSerializer();
        String topic = properties.getProperty("topic", "cdc-events");

        int linesRead = 0;
        CountingCallback callback = new CountingCallback();

        long start = System.nanoTime();
        try (RandomAccessFile raf = new RandomAccessFile(inputFile, "r");
             KafkaProducer<String, String> producer = new KafkaProducer<>(properties, serializer, serializer)) {

            String line;
            while ((line = raf.readLine()) != null) {
                linesRead++;
                producer.send(new ProducerRecord<>(topic, line), callback);
            }
        }

        long timeTaken = System.nanoTime() - start;
        System.out.printf("""
            Lines read from file: %d
            Messages written to Kafka: %d
            Time taken: %s%n""", linesRead, callback.count, Utils.formatTime(timeTaken));
    }

    private static String getInputFile(Properties properties) {
        String inputFile = properties.getProperty(inputFileKey);
        Path input;
        if (inputFile == null) {
            System.out.println("Property '" + inputFileKey + "' is not found - reading `stream.jsonl`");
            input = Resources.fileInProjectRoot("stream.jsonl");
        } else {
            input = Paths.get(inputFile);
        }

        boolean fileExists = Files.exists(input) && Files.isRegularFile(input);
        if (!fileExists) {
            System.err.println("Expected regular file to read from - got:" + inputFile);
            System.exit(1);
        }

        String result = input.toFile().getAbsolutePath();
        System.out.println("To load records from: " + result);
        return result;
    }

    private static final class CountingCallback implements Callback {
        int count;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null) {
                System.err.println("error encountered: " + e.getMessage());
            } else {
                count++;
            }
        }
    }
}
