package com.mj.esclient.demo.lowlevelclient

import com.mj.esclient.demo.BaseTest
import org.apache.http.client.methods.HttpHead
import org.elasticsearch.client.Request
import org.elasticsearch.client.Response
import org.junit.jupiter.api.Test

import static java.net.HttpURLConnection.HTTP_NOT_FOUND

class LowLevelClientTests extends BaseTest{
    @Test
    void aliasExist7xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String aliasName = "doc_alias_${uniqueSuffix}"
        String indexName = "doc_${uniqueSuffix}"

        createIndex7x(indexName,getIndexSettings(), getDocumentMapping())
        addIndexToAlias7x(indexName, aliasName)

        Request request = new Request(HttpHead.METHOD_NAME, aliasName)
        Response response = elasticsearch7xClient.elasticsearchLowLevelClient().performRequest(request)
        assert response.statusLine.statusCode != HTTP_NOT_FOUND
    }

    @Test
    void aliasExist8xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String aliasName = "doc_alias_${uniqueSuffix}"
        String indexName = "doc_${uniqueSuffix}"

        createIndex8x(indexName,getIndexSettings(), getDocumentMapping())
        addIndexToAlias8x(indexName, aliasName)

        Request request = new Request(HttpHead.METHOD_NAME, aliasName)
        Response response = elasticsearch8xClient.elasticsearchLowLevelClient().performRequest(request)
        assert response.statusLine.statusCode != HTTP_NOT_FOUND
    }
}
