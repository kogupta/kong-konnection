package org.konnect;


import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
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
import java.util.ArrayList;
import java.util.List;

public final class ESWriter {
    private ESWriter() {}

    public static void main(String[] args) throws IOException {
//        ThreadFactory tf = ThreadUtils.createThreadFactory("es-writer", false);
//        try (ExecutorService executor = Executors.newSingleThreadExecutor(tf);
//             HttpClient client = HttpClient.newBuilder().executor(executor).build()) {
//            HttpRequest request = HttpRequest.newBuilder()
//                                      .POST(HttpRequest.BodyPublishers.ofString(, StandardCharsets.UTF_8))
//                                      .uri(URI.create("http://localhost:9200/cdc/_bulk?pretty&refresh"))
//                                      .build();
//            client.send(request, );
//        }

        List<String> lines = new ArrayList<>();

        HttpHost httpHost = new HttpHost("http", "localhost", 9200);
        ApacheHttpClient5Transport transport = ApacheHttpClient5TransportBuilder.builder(httpHost)
                                                   .setMapper(new JacksonJsonpMapper())
                                                   .build();
        OpenSearchClient client = new OpenSearchClient(transport);

        String indexName = "cdc";

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
        BulkResponse bulkResponse = client.bulk(bulkReq.build());

        for (BulkResponseItem responseItem : bulkResponse.items()) {
            if (responseItem.status() == HttpStatus.SC_TOO_MANY_REQUESTS) {

            }
        }


    }
}
