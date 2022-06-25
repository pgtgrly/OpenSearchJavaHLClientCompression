package com.amazonaws.samples;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AmazonOpenSearchServiceClient {

    private static final String serviceName = "es";
    private static final String region = ""; // e.g. us-west-1
    private static final String host = ""; // e.g. https://search-mydomain.us-west-1.es.amazonaws.com
    private static final String type = "_doc";
    
    static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    
    public static void main(String[] args) throws IOException {
        
        RestHighLevelClient searchClient = searchClient();
        
        String index = "java-client-test-index";
        
        // Create a document that simulates a simple log line from a web server
        Map<String, Object> document = new HashMap<>();
        document.put("method", "GET");
        document.put("client_ip_address", "123.456.78.90");
        document.put("timestamp", "10/Oct/2000:14:56:14 -0700");
        
        System.out.println("Demoing a single index request:");
        String id = "1";
        IndexRequest indexRequest = new IndexRequest(index, type, id).source(document);
        IndexResponse indexResponse = searchClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
        
        System.out.println("Demoing a 1 MB bulk request:");
        BulkRequest bulkRequest = new BulkRequest();
        
        // Add documents (the simple log line from earlier) to the request until it exceeds 1 MB
        while (bulkRequest.estimatedSizeInBytes() < 1000000) {
            // By not specifying an ID, these documents get auto-assigned IDs
            bulkRequest.add(new IndexRequest(index, type).source(document));
        }
        
        try {
            // Send the request and get the response
            BulkResponse bulkResponse = searchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

            // Check the response for failures
            if (bulkResponse.hasFailures()) {
                System.out.println("Encountered failures:");
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        System.out.println(bulkItemResponse.getFailureMessage());
                    }
                }
            }
            else {
                System.out.println("No failures!");
                // Uncomment these lines for a line-by-line summary
//                for (BulkItemResponse bulkItemResponse : bulkResponse) {
//                    System.out.println(bulkItemResponse.getResponse().toString());
//                }
            }
        }
        
        // Usually happens when the request size is too large
        catch (OpenSearchException e) {
            System.out.println("Encountered exception:");
            System.out.println(e.toString());
        }
    }
        
    // Adds the interceptor to the OpenSearch REST client
    public static RestHighLevelClient searchClient() {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(serviceName);
        signer.setRegionName(region);
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
        return new RestHighLevelClient(RestClient.builder(HttpHost.create(host)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
    }
}