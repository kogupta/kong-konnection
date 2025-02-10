package org.konnect;


import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.konnect.utils.Resources;
import org.konnect.utils.Utils;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class ESWriter {
    private ESWriter() {}

    public static void main(String[] args) throws IOException, URISyntaxException {
        // usage: ESWriter $configFile

        Properties props = args.length < 1 ? loadFromClasspath() : Utils.loadPropsOrExit(args[0]);

        OpenSearchClient client = createClient(props);
        String indexName = props.getProperty("index", "cdc");

        StringDeserializer strDeserializer = new StringDeserializer();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, strDeserializer, strDeserializer);

        String topic = props.getProperty("topic", "cdc-events");
        consumer.subscribe(List.of(topic));

        List<String> lines = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                lines.add(record.value());
            }

            esBulkIndex(lines, indexName, client);
        }
    }

    private static void esBulkIndex(List<String> lines, String indexName, OpenSearchClient client) throws IOException {
        BulkRequest bulkRequest = createBulkReq(lines, indexName);
        BulkResponse bulkResponse = client.bulk(bulkRequest);

        for (BulkResponseItem responseItem : bulkResponse.items()) {
            if (responseItem.status() == HttpStatus.SC_TOO_MANY_REQUESTS) {

            }
        }
    }

    private static BulkRequest createBulkReq(List<String> lines, String indexName) {
        List<BulkOperation> ops = new ArrayList<>(lines.size());

        for (String line : lines) {
            ops.add(
                new BulkOperation.Builder().index(
                    IndexOperation.of(builder -> builder.document(line))
                ).build());
        }

        BulkRequest.Builder bulkReq = new BulkRequest.Builder()
                                          .index(indexName)
                                          .operations(ops)
                                          .refresh(Refresh.WaitFor);
        return bulkReq.build();
    }

    private static OpenSearchClient createClient(Properties props) throws URISyntaxException {
        HttpHost[] result = parseHosts(props);

        ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder.builder(result)
                                                   .setMapper(new JacksonJsonpMapper())
                                                   .build();
        OpenSearchClient client = new OpenSearchClient(transport);
        return client;
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

    private static Properties loadFromClasspath() {
        System.out.println("""
            Usage: ESWriter /path/to/config/file.properties
                Reading `resources/esWriter.properties` instead""");

        return Resources.loadFromClasspath("esWriter.properties");
    }
}
