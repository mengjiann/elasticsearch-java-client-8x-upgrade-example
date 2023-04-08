package com.mj.esclient.demo.client


import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders
import co.elastic.clients.json.JsonData
import com.mj.esclient.demo.BaseTest
import org.apache.lucene.search.join.ScoreMode
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.ExistsQueryBuilder
import org.elasticsearch.index.query.NestedQueryBuilder
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.index.query.WildcardQueryBuilder

class QueriesTests extends BaseTest {

    /**
     * NestedQuery
     */
    void nestedQuery7xTest() {
        def query = new NestedQueryBuilder("path",
                new TermQueryBuilder("path.nestedPath", "value"),
                ScoreMode.Avg)
    }

    void nestedQuery8xTest() {
        def termQuery = QueryBuilders.term { term ->
            term.field("path.nestedPath")
                    .value("value")
        }
        def query = QueryBuilders.nested { nested ->
            nested.path("path")
                    .query(termQuery)
                    .scoreMode(ChildScoreMode.Avg)
        }
    }

    /**
     * BoolQuery
     */
    void boolQuery7xTest() {
        def query = new BoolQueryBuilder()
        query.should(new TermQueryBuilder("field", "value"))
    }

    void boolQuery8xTest() {
        def query = QueryBuilders.bool { bool ->
            bool.should { should ->
                should.term { term ->
                    term.field("field")
                            .value("value")
                }
            }
        }
    }

    /**
     * WildcardQuery
     */
    void wildcardQuery7xTest() {
        def query = new WildcardQueryBuilder("field", "*value*")
                .caseInsensitive(true)
    }

    void wildcardQuery8xTest() {
        def query = QueryBuilders.wildcard { wildcard ->
            wildcard.field("field")
                    .value("*value*")
                    .caseInsensitive(true)
        }
    }

    /**
     * ExistsQuery
     */
    void existQuery7xTest() {
        def query = new ExistsQueryBuilder("field")
    }

    void existQuery8xTest() {
        def query = QueryBuilders.exists {
            it.field("field")
        }
    }

    /**
     * RangeQuery
     */
    void rangeQuery7xTest(){
        def queryBuilder = new RangeQueryBuilder("field")
        queryBuilder.gte("value ||")
        queryBuilder.lt("value ||")
    }

    void rangeQuery8xTest(){
        def query = QueryBuilders.range {
            it.field("field")
            .gte(JsonData.of("value ||"))
            .lt(JsonData.of("value ||"))
        }
    }
}
