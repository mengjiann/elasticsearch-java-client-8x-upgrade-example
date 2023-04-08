package com.mj.esclient.demo

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.cluster.ElasticsearchClusterClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient

class Elasticsearch8xClient {

    private static final int ES_PORT = 9200
    private static final String ES_HOST = "localhost"

    Elasticsearch8xClient() {
    }

    RestClient elasticsearchLowLevelClient(){
        return RestClient.builder(new HttpHost(ES_HOST, ES_PORT)).build()
    }

    RestClientTransport restClientTransport(){
        return new RestClientTransport(
                elasticsearchLowLevelClient(), elasticsearchJacksonJsonpMapper())
    }

    JacksonJsonpMapper elasticsearchJacksonJsonpMapper(){
        return new JacksonJsonpMapper()
    }

    ElasticsearchClient elasticsearchClient(){
        return new ElasticsearchClient(restClientTransport())
    }

    ElasticsearchClusterClient elasticsearchClusterClient(){
        return new ElasticsearchClusterClient(restClientTransport())
    }
}
