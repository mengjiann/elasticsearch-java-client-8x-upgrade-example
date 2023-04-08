package com.mj.esclient.demo

import org.apache.http.HttpHost
import org.elasticsearch.client.ClusterClient
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient

class Elasticsearch7xClient {

    private static final int ES_PORT = 9201
    private static final String ES_HOST = "localhost"

    Elasticsearch7xClient() {
    }

    RestClient elasticsearchLowLevelClient(){
        return elasticsearchClient().lowLevelClient
    }


    RestHighLevelClient elasticsearchClient(){
        return new RestHighLevelClient(RestClient.builder(new HttpHost(ES_HOST, ES_PORT, "http")))
    }

    ClusterClient elasticsearchClusterClient(){
        return this.elasticsearchClient().cluster()
    }

}
