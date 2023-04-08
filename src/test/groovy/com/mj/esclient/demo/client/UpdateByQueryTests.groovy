package com.mj.esclient.demo.client

import co.elastic.clients.elasticsearch._types.Script
import co.elastic.clients.elasticsearch._types.ScriptLanguage
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders
import co.elastic.clients.elasticsearch.core.UpdateByQueryRequest
import co.elastic.clients.transport.rest_client.RestClientOptions
import com.mj.esclient.demo.BaseTest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.script.ScriptType
import org.junit.jupiter.api.Test

class UpdateByQueryTests extends BaseTest{

    @Test
    void sendUpdateByQueryRequest7xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"

        this.createIndex7x(indexName, getIndexSettings(), getDocumentMapping())
        this.bulkIndexDocWithIndexName7x(5, indexName)

        org.elasticsearch.index.reindex.UpdateByQueryRequest request = new org.elasticsearch.index.reindex.UpdateByQueryRequest()
        request.indices(indexName)

//        org.elasticsearch.script.Script script = new Script(ScriptType.INLINE, "painless", "", [:])
//        request.setScript(script)

        def query = new MatchAllQueryBuilder()
        request.setQuery(query)

        def requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addParameter("conflicts", "proceed")
                .addParameter("refresh", "true")

        elasticsearch7xClient.elasticsearchClient().updateByQuery(request, requestOptions.build())
    }

    @Test
    void sendUpdateByQueryRequest8xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"

        this.createIndex8x(indexName, getIndexSettings(), getDocumentMapping())
        this.bulkIndexDocWithIndexName8x(5, indexName)

        UpdateByQueryRequest.Builder builder = new UpdateByQueryRequest.Builder()
        builder.index(indexName)

//        Script script = new Script.Builder().inline{sb ->
//            sb.lang(ScriptLanguage.Painless.jsonValue()).source("").params([:])
//        }.build()
//        builder.script(script)

        builder.query(QueryBuilders.matchAll().build()._toQuery())


        def requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addParameter("conflicts", "proceed")
                .addParameter("refresh", "true")

        elasticsearch8xClient.elasticsearchClient()
                .withTransportOptions(new RestClientOptions(requestOptions.build()))
                .updateByQuery(builder.build())
    }

}
