package com.mj.esclient.demo

import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.json.JsonData
import groovy.json.JsonOutput
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Request
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.RequestOptions
import org.junit.jupiter.api.BeforeEach

import java.time.LocalDateTime

class BaseTest {
    protected Elasticsearch7xClient elasticsearch7xClient
    protected Elasticsearch8xClient elasticsearch8xClient

    @BeforeEach
    void setup() {
        elasticsearch7xClient = new Elasticsearch7xClient()
        elasticsearch8xClient = new Elasticsearch8xClient()
    }

    /**
     * Create new index with setting and mapping
     */
    void createIndex7x(String indexName, Map indexSettings, Map mappings) {
        CreateIndexRequest request = new CreateIndexRequest(indexName)
        request.settings(indexSettings)
        request.mapping(mappings)
        elasticsearch7xClient.elasticsearchClient().indices().create(request, RequestOptions.DEFAULT)
    }

    void createIndex8x(String indexName, Map indexSettings, Map mappings) {
        def defaultJsonpMapper = elasticsearch8xClient.elasticsearchJacksonJsonpMapper()
        elasticsearch8xClient.elasticsearchClient().indices().create({ builder ->
            builder.index(indexName)
                    .settings({ settingsBuilder ->
                        settingsBuilder.withJson(
                                new StringReader(JsonData.of(indexSettings, defaultJsonpMapper).toJson().toString())
                        )
                    })
                    .mappings({ mappingsBuilder ->
                        mappingsBuilder.withJson(
                                new StringReader(JsonData.of(mappings, defaultJsonpMapper).toJson().toString())
                        )
                    })
        })
    }

    /**
     * Add index to alias
     */
    void addIndexToAlias7x(String index, String alias) {
        elasticsearch7xClient.elasticsearchLowLevelClient().performRequest(buildCreateAliasRequest(index, alias))
    }

    void addIndexToAlias8x(String index, String alias) {
        elasticsearch8xClient.elasticsearchLowLevelClient().performRequest(buildCreateAliasRequest(index, alias))
    }

    /**
     * Bulk index
     */
    void bulkIndexDocWithIndexName7x(int documentCount, String indexName){
        org.elasticsearch.action.bulk.BulkRequest bulkRequest = new org.elasticsearch.action.bulk.BulkRequest()
        DocumentBuilder.buildDocuments(documentCount).forEach {doc ->
            bulkRequest.add(new IndexRequest(indexName)
                    .id(doc.id)
                    .source(doc.toMap()))
        }
        elasticsearch7xClient.elasticsearchClient().bulk(bulkRequest, RequestOptions.DEFAULT)
    }

    void bulkIndexDocWithIndexName8x(int documentCount, String indexName){
        BulkRequest.Builder builder = new BulkRequest.Builder()
        DocumentBuilder.buildDocuments(documentCount).forEach {doc ->
            builder.operations {op ->
                op.index{index ->
                    index.index(indexName)
                            .id(doc.id)
                            .document(doc)
                }
            }
        }
        BulkRequest bulkRequest = builder.build()
        elasticsearch8xClient.elasticsearchClient().bulk(bulkRequest)
    }

    /**
     * Others common methods
     */
    private static Request buildCreateAliasRequest(String index, String alias) {
        Request request = new Request(HttpPost.METHOD_NAME, "_aliases")
        Map requestContent = [
                actions: [
                        add: [
                                index: index,
                                alias: alias
                        ]
                ]
        ]
        request.setEntity(new NStringEntity(JsonOutput.toJson(requestContent), ContentType.APPLICATION_JSON))
        return request
    }

    static String getUniqueSuffix() {
        return LocalDateTime.now().format("yyyyMMddHHmmss")
    }

    static Map getDocumentMapping() {
        return ["properties": [
                "id"       : ["type": "keyword"],
                "name"     : ["type": "keyword"],
                "metadata1": ["type": "keyword"],
                "metadata2": ["type": "keyword"],
        ]
        ]

    }

    static Map getIndexSettings() {
        return [
                number_of_shards: 1,
                index           : [
                        mapping: [
                                total_fields: [
                                        limit: 10
                                ]
                        ]
                ]
        ]
    }
}
