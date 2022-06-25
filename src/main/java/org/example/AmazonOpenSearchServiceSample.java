package org.example;

import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor2;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import java.io.IOException;
        import java.util.HashMap;
        import java.util.Map;

public class AmazonOpenSearchServiceSample {

    private static String serviceName = "es";
    private static String region = "us-west-2"; // e.g. us-east-1
    private static String host = "https://search-compression-client-test-wjvs7pttvndq3nhffx4ksqy6cy.us-west-2.es.amazonaws.com";
    private static String index = "my-index";
    private static String type = "_doc";
    private static String id = "1";

    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    public static void main(String[] args) throws IOException {
        RestHighLevelClient searchClient = searchClient(serviceName, region);
        // Create the document as a hash map
        Map<String, Object> document = new HashMap<>();
        document.put("title", "Walk the Line");
        document.put("director", "James Mangold");
        document.put("year", "2005");

        // Form the indexing request, send it, and print the response
        IndexRequest request = new IndexRequest(index).id(id).source(document);
        IndexResponse response = searchClient.index(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    // Adds the interceptor to the OpenSearch REST client
    public static RestHighLevelClient searchClient(String serviceName, String region) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        //HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor2(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(host)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)).setCompressionEnabled(true));
    }
}