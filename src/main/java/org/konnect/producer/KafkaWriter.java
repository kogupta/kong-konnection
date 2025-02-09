package org.konnect.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.konnect.utils.Utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class KafkaWriter {
    private static final String inputFileKey = "inputFile";

    private KafkaWriter() {}

    public static void main(String[] args) throws IOException {
        // usage: KafkaWriter $configFile
        if (args.length < 1) {
            System.err.println("Usage: KafkaWriter /path/to/config/file.properties");
            System.exit(1);
        }

        startWithPropsFile(args[0]);
    }

    public static void startWithPropsFile(String absolutePath) throws IOException {
        Properties properties = readFromFile(absolutePath);
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

    private static Properties readFromFile(String absolutePath) throws IOException {
        Properties properties = new Properties();
        byte[] bytes = Files.readAllBytes(Paths.get(absolutePath));
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        properties.load(is);
        return properties;
    }

    private static String getInputFile(Properties properties) {
        String inputFile = properties.getProperty(inputFileKey);
        if (inputFile == null) {
            System.err.println("Property '" + inputFileKey + "' is not found - exiting ...");
            System.exit(1);
        }

        Path input = Paths.get(inputFile);
        boolean fileExists = Files.exists(input) && Files.isRegularFile(input);
        if (!fileExists) {
            System.err.println("Expected regular file to read from - got:" + inputFile);
            System.exit(1);
        }

        System.out.println("To load records from: " + inputFile);
        return inputFile;
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
