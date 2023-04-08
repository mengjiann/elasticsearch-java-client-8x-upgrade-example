package com.mj.esclient.demo.client


import com.mj.esclient.demo.BaseTest
import org.junit.jupiter.api.Test

class BulkRequestTests extends BaseTest{

    @Test
    void bulkIndex7xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"
        this.bulkIndexDocWithIndexName7x(5, indexName)
    }

    @Test
    void bulkIndex8xTest(){
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"
        this.bulkIndexDocWithIndexName8x(5, indexName)
    }
}
