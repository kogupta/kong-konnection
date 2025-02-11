package org.konnect;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.konnect.utils.Resources;
import org.konnect.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class KafkaWriter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaWriter.class);
    private static final String inputFileKey = "inputFile";

    private KafkaWriter() {}

    public static void main(String[] args) throws IOException {
        // usage: KafkaWriter $configFile
        Properties props = args.length < 1 ? loadFromClasspath() : Utils.loadPropsOrExit(args[0]);
        start(props);
    }

    private static Properties loadFromClasspath() {
        logger.warn("""
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
        logger.info("""
            Lines read from file: {}
            Messages written to Kafka: {}
            Time taken: {}""", linesRead, callback.count, Utils.formatTime(timeTaken));
    }

    private static String getInputFile(Properties properties) {
        String inputFile = properties.getProperty(inputFileKey);
        Path input;
        if (inputFile == null) {
            logger.warn("Property '{}' is not found - reading `stream.jsonl`", inputFileKey);
            input = Resources.fileInProjectRoot("stream.jsonl");
        } else {
            input = Paths.get(inputFile);
        }

        boolean fileExists = Files.exists(input) && Files.isRegularFile(input);
        if (!fileExists) {
            logger.error("Expected regular file to read from - got: {}", inputFile);
            System.exit(1);
        }

        String result = input.toFile().getAbsolutePath();
        logger.info("To load records from: {}", result);
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
