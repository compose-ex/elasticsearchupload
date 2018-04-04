package com.compose.es.bulkuploader;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class BulkUpload {

    public static void main(String[] args) {
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
               new HttpHost(host, port, protocol))
                 .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));

        try {
            String indexName="enron";
            Response response = client.getLowLevelClient().performRequest("HEAD", "/" + indexName);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 404) {
                CreateIndexRequest cireq=new CreateIndexRequest(indexName);
                CreateIndexResponse ciresp=client.indices().create(cireq);
                System.out.println("Created index");
            } else {
                System.out.println("Index exists");
            }

            Stream<String> stream= Files.lines(Paths.get("enron.json"));

            BulkRequest request=new BulkRequest();

            int count=0;
            int batch=15000;

            BufferedReader br=new BufferedReader(new FileReader("enron.json"));

            String line;

            while((line=br.readLine())!=null) {
                request.add(new IndexRequest(indexName,"mail").source(line, XContentType.JSON));
                count++;
                if(count%batch==0) {
                    BulkResponse bulkresp=client.bulk(request);
                    if(bulkresp.hasFailures()) {
                        for (BulkItemResponse bulkItemResponse : bulkresp) {
                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                System.out.println("Error"+failure.toString());
                            }                        }
                        } else {
                        System.out.println(batch+" items onward! " + count);
                    }

                    request=new BulkRequest();
                }
            }
            BulkResponse bulkresp=client.bulk(request);
            if(bulkresp.hasFailures()) {
                for (BulkItemResponse bulkItemResponse : bulkresp) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        System.out.println("Error"+failure.toString());
                    }                        }
            } else {
                System.out.println("And done with "+count);
            }
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
