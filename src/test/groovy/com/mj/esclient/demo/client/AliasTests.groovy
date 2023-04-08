package com.mj.esclient.demo.client

import co.elastic.clients.elasticsearch.indices.GetAliasResponse
import com.mj.esclient.demo.BaseTest
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.client.GetAliasesResponse
import org.elasticsearch.client.RequestOptions
import org.junit.jupiter.api.Test

class AliasTests extends BaseTest{

    @Test
    void createAndGetAlias7xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String aliasName = "doc_alias_${uniqueSuffix}"
        String indexName = "doc_${uniqueSuffix}"

        createIndex7x(indexName,getIndexSettings(), getDocumentMapping())
        addIndexToAlias7x(indexName, aliasName)

        GetAliasesRequest request = new GetAliasesRequest(aliasName)
        GetAliasesResponse response =  elasticsearch7xClient.elasticsearchClient().indices().getAlias(request, RequestOptions.DEFAULT)
        assert response.aliases.keySet().toList().contains(indexName)
    }

    @Test
    void createAndGetAlias8xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String aliasName = "doc_alias_${uniqueSuffix}"
        String indexName = "doc_${uniqueSuffix}"

        createIndex8x(indexName,getIndexSettings(), getDocumentMapping())
        addIndexToAlias8x(indexName, aliasName)

        GetAliasResponse response =  elasticsearch8xClient.elasticsearchClient().indices().getAlias({ builder ->
            builder.name(aliasName)
        })
        assert response.result().keySet().toList().contains(indexName)
    }


}
