package com.mj.esclient.demo.client

import co.elastic.clients.elasticsearch._types.SearchType
import co.elastic.clients.elasticsearch._types.SortOptions
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.mapping.FieldType
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.core.search.HighlightField
import co.elastic.clients.elasticsearch.core.search.Hit
import com.mj.esclient.demo.BaseTest

import org.elasticsearch.client.RequestOptions
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.FieldSortBuilder
import org.junit.jupiter.api.Test


class SearchQueryTests extends BaseTest {

    @Test
    void searchQuery7xTest() {
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"

        this.createIndex7x(indexName, getIndexSettings(), getDocumentMapping())
        this.bulkIndexDocWithIndexName7x(5, indexName)

        org.elasticsearch.action.search.SearchRequest searchRequest =
                new org.elasticsearch.action.search.SearchRequest(indexName)
        searchRequest.searchType(org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH)

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()

        searchSourceBuilder.from(0)
        searchSourceBuilder.size(10)
        searchSourceBuilder.explain(true)

        // sort
        searchSourceBuilder.sort(new FieldSortBuilder("id")
                .unmappedType("keyword").order(org.elasticsearch.search.sort.SortOrder.ASC))

        // source fields
        searchSourceBuilder.fetchSource(new String[]{"id", "name"}, null)

        // highlight
        HighlightBuilder highlightBuilder = searchSourceBuilder.highlight()
        ["metadata1", "metadata2"].each { fieldName ->
            highlightBuilder.field(fieldName)
        }
        searchSourceBuilder.highlighter(highlightBuilder)

        searchSourceBuilder.query(new MatchAllQueryBuilder())
        searchRequest.source(searchSourceBuilder)

        org.elasticsearch.action.search.SearchResponse searchResponse = elasticsearch7xClient.elasticsearchClient()
                .search(searchRequest, RequestOptions.DEFAULT)
        SearchHit[] searchResults = searchResponse.hits.hits

        // get hits
        def totalHits = searchResponse.hits.getTotalHits().value

        // cast as map
        def docIds = searchResults*.getSourceAsMap().collect { it.id }
    }

    @Test
    void searchQuery8xTest() {
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"

        this.createIndex8x(indexName, getIndexSettings(), getDocumentMapping())
        this.bulkIndexDocWithIndexName8x(5, indexName)

        SearchRequest.Builder searchRequestBuilder = new SearchRequest.Builder().index(indexName)

        searchRequestBuilder.searchType(SearchType.DfsQueryThenFetch)
        searchRequestBuilder.from(0)
        searchRequestBuilder.size(10)
        searchRequestBuilder.explain(true)

        // sort
        searchRequestBuilder.sort(SortOptions.of(s -> s
                .field(f -> f
                        .field("id")
                        .unmappedType(FieldType.Keyword)
                        .order(SortOrder.Asc))
        ))

        // source filter
        searchRequestBuilder.source({ source ->
            source.filter({ filter ->
                filter.includes(["id", "name"])
            })
        })

        // highlight
        Map<String, HighlightField> highlightFieldMap = [:]
        ["metadata1", "metadata2"].each { String fieldName ->
            highlightFieldMap.put(fieldName, HighlightField.of(hf -> hf))
        }
        searchRequestBuilder.highlight({ builder ->
            builder.fields(highlightFieldMap)
        })

        searchRequestBuilder.query(QueryBuilders.matchAll().build()._toQuery())

        SearchResponse searchResponse = elasticsearch8xClient.elasticsearchClient()
                .search(searchRequestBuilder.build(), Map.class)
        Hit[] searchResults = searchResponse.hits().hits()

        // totalHits
        def totalHits = searchResponse.hits().total().value()

        // already in map
        def docIds = searchResults*.source().collect { it.id }
    }
}
