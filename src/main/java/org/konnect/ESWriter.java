package org.konnect;


import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.konnect.utils.Resources;
import org.konnect.utils.Utils;
import org.opensearch.client.json.JsonpDeserializable;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public final class ESWriter {
    private static final Logger logger = LoggerFactory.getLogger(ESWriter.class);

    private ESWriter() {}

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // usage: ESWriter $configFile

        Properties props = args.length < 1 ? loadFromClasspath() : Utils.loadPropsOrExit(args[0]);

        String indexName = props.getProperty("index", "cdc");
        Indexer indexer = new Indexer(props, indexName);

        String topic = props.getProperty("topic", "cdc-events");
        CountDownLatch latch = new CountDownLatch(1);
        KConsumer kConsumer = new KConsumer(latch, props, topic, indexer);

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            executor.submit(kConsumer);
            latch.await();
        }
    }

    private static final class KConsumer implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final CountDownLatch latch;
        private final Consumer<List<String>> msgConsumer;

        private volatile boolean run;

        KConsumer(CountDownLatch latch,
                  Properties props,
                  String topic,
                  Consumer<List<String>> msgConsumer) {
            this.latch = latch;
            this.msgConsumer = msgConsumer;

            StringDeserializer strDeserializer = new StringDeserializer();
            consumer = new KafkaConsumer<>(props, strDeserializer, strDeserializer);
            consumer.subscribe(List.of(topic));

            run = true;
        }


        @Override
        public void run() {
            Thread.currentThread().setName("kafka -> es");

            List<String> lines = new ArrayList<>();
            while (run) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                lines.clear();

                for (ConsumerRecord<String, String> record : records) {
                    lines.add(record.value());
                }

                logger.info("KConsumer#run: fetched {} lines", lines.size());
                if (!lines.isEmpty())
                    msgConsumer.accept(lines);
            }

            latch.countDown();
        }

        public void stop() {
            run = false;
        }
    }

    private static final class Indexer implements Consumer<List<String>> {
        private static final int maxRetries = 5;

        private final String indexName;
        private final OpenSearchClient client;
        private final ExponentialTimeDelay delay;

        Indexer(Properties props, String indexName) throws URISyntaxException {
            this.indexName = indexName;
            this.client = createClient(props);
            delay = new ExponentialTimeDelay();
        }

        @Override
        public void accept(List<String> lines) {
            int retryAttempts = 0;

            while (retryAttempts <= maxRetries) {
                List<String> failedDocs = bulkRequest(lines);
                if (failedDocs.isEmpty()) {
                    logger.info("Indexer#accept: indexed {} lines", lines.size());
                    return;
                }

                retryAttempts++;
                logger.warn("Indexer#accept: indexed {} lines, retry attempt {}",
                    lines.size() - failedDocs.size(), retryAttempts);
                lines = failedDocs;
                Utils.uncheckedSleep(delay.getDelay(retryAttempts));
            }
        }

        private List<String> bulkRequest(List<String> lines) {
            BulkRequest bulkRequest = createBulkReq(lines, indexName);
            try {
                BulkResponse bulkResponse = client.bulk(bulkRequest);

                // ⚠ extremely janky "error" handling 🤮
                // - not proper by any stretch of imagination
                // - but opensearch java api docs are abject/awful - so dont know any better
                List<String> failedDocs = new ArrayList<>(lines.size());

                if (bulkResponse.errors()) {
                    logger.error("Indexer#bulkRequest: bulk request has errors");
                    for (BulkResponseItem item : bulkResponse.items()) {
                        if (item.error() != null) {
                            logger.error(item.error().toJsonString());
                        }
                    }
                }

                List<BulkResponseItem> items = bulkResponse.items();
                for (int i = 0; i < items.size(); i++) {
                    BulkResponseItem responseItem = items.get(i);
                    if (responseItem.status() == HttpStatus.SC_TOO_MANY_REQUESTS) {
                        failedDocs.add(lines.get(i));
                    }
                }
                return failedDocs;
            } catch (IOException e) {
                logger.error("Indexer#accept: error while indexing", e);
                return lines;
            }
        }

        private BulkRequest createBulkReq(List<String> lines, String indexName) {
            BulkRequest.Builder br = new BulkRequest.Builder();

            for (String line : lines) {
                JsonBinary data = JsonBinary.of(line);

                br.operations(
                    op -> op.index(
                        idx -> idx.index(indexName).document(data)));
            }

            return br.build();
        }

        private static OpenSearchClient createClient(Properties props) throws URISyntaxException {
            HttpHost[] result = parseHosts(props);

            ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder.builder(result)
                                                       .setMapper(new JacksonJsonpMapper())
                                                       .build();
            return new OpenSearchClient(transport);
        }

        private static HttpHost[] parseHosts(Properties props) throws URISyntaxException {
            String s = props.getProperty("hosts", "http://localhost:9200");
            String[] split = s.split(",");
            HttpHost[] result = new HttpHost[split.length];
            for (int i = 0; i < split.length; i++) {
                result[i] = HttpHost.create(split[i]);
            }
            return result;
        }
    }

    private static final class ExponentialTimeDelay {
        private static final int base = 2;
        private static final long maxBackOffTime = 30_000;  // 30 seconds

        private final ThreadLocalRandom rand = ThreadLocalRandom.current();

        public long getDelay(int noAttempts) {
            double pow = Math.pow(base, noAttempts);
            int extraDelay = rand.nextInt(1000);
            return (long) Math.min(pow * 1000 + extraDelay, maxBackOffTime);
        }
    }

    private static Properties loadFromClasspath() {
        logger.warn("""
            Usage: ESWriter /path/to/config/file.properties
                Reading `resources/esWriter.properties` instead""");

        return Resources.loadFromClasspath("esWriter.properties");
    }

    // Nowhere is it mentioned in opensearch docs what to do in case of json strings!
    // details obtained from elastic docs
    @JsonpDeserializable
    record JsonBinary(byte[] bytes, String contentType) {
        public static JsonBinary of(String json) {
            return new JsonBinary(json.getBytes(StandardCharsets.UTF_8), "application/json");
        }
    }
}
