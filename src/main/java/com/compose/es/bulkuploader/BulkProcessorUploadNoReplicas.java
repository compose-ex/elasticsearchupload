package com.compose.es.bulkuploader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BulkProcessorUploadNoReplicas {

    public static void main(String[] args) {
        long t = System.currentTimeMillis();

        URL url = null;

        try {
            url = new URL(System.getenv("COMPOSE_ELASTICSEARCH_URL"));
        } catch (MalformedURLException me) {
            System.err.println("COMPOSE_ELASTICSEARCH_URL not present or malformed");
            System.exit(1);
        }

        String host = url.getHost();
        int port = url.getPort();
        String user = url.getUserInfo().split(":")[0];
        String password = url.getUserInfo().split(":")[1];
        String protocol = url.getProtocol();

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, protocol)).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));

        try {
            String indexName = "enron";
            Response response = client.getLowLevelClient().performRequest("HEAD", "/" + indexName);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 404) {
                CreateIndexRequest cireq = new CreateIndexRequest(indexName);
                cireq.settings(Settings.builder().put("index.number_of_replicas", 0));
                CreateIndexResponse ciresp = client.indices().create(cireq);
                System.out.println("Created index");
            } else {
                System.out.println("Index exists");
            }


            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                int count = 0;

                @Override
                public void beforeBulk(long l, BulkRequest bulkRequest) {
                    count = count + bulkRequest.numberOfActions();
                    System.out.println("Uploaded " + count + " so far");
                }

                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures()) {
                        for (BulkItemResponse bulkItemResponse : bulkResponse) {
                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                System.out.println("Error " + failure.toString());
                            }
                        }
                    }
                }

                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                    System.out.println("Big errors " + throwable.toString());
                }
            };

            BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();

            BufferedReader br = new BufferedReader(new FileReader("enron.json"));

            String line;

            while ((line = br.readLine()) != null) {
                bulkProcessor.add(new IndexRequest(indexName, "mail").source(line, XContentType.JSON));
            }

            System.out.println("Waiting to finish");

            String jsonString = "{ \"index\": { \"number_of_replicas\": 1 } }";
            Map<String, String> params = Collections.emptyMap();
            HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
            Response resetresponse = client.getLowLevelClient().performRequest("PUT", "/" + indexName + "/_settings", null, entity);

            boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
            if (!terminated) {
                System.out.println("Some requests have not been processed");
            }

            client.close();
            long tn = System.currentTimeMillis();
            System.out.println("Took " + (tn - t) / 1000 + " seconds");


        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
