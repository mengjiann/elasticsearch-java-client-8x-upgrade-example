package com.mj.esclient.demo.client

import co.elastic.clients.elasticsearch._types.mapping.Property
import co.elastic.clients.elasticsearch.indices.GetMappingRequest
import co.elastic.clients.elasticsearch.indices.GetMappingResponse
import co.elastic.clients.json.JsonData
import com.mj.esclient.demo.BaseTest
import com.mj.esclient.demo.Document
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse
import org.elasticsearch.client.indices.GetMappingsRequest
import org.elasticsearch.client.indices.GetMappingsResponse
import org.elasticsearch.client.indices.PutMappingRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.settings.Settings
import org.junit.jupiter.api.Test

class IndicesTests extends BaseTest {

    @Test
    void updateIndexSettings7xTest() {
        String newIndexName = "${Document.INDEX_NAME}${getUniqueSuffix()}"
        this.createIndex7x(newIndexName, getIndexSettings(), getDocumentMapping())

        List<String> indexNames = [newIndexName]
        String fieldLimitSettingKey = "index.mapping.total_fields.limit"

        GetSettingsRequest request = new GetSettingsRequest()
        request.indices(*indexNames)
        GetSettingsResponse response = elasticsearch7xClient.elasticsearchClient().indices().getSettings(request, RequestOptions.DEFAULT)
        int currentLimit = response.getSetting(indexNames.first(), fieldLimitSettingKey).toInteger()

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest()
        updateSettingsRequest.indices(*indexNames)
        Settings settings = Settings.builder().put(fieldLimitSettingKey, currentLimit * 2).build()
        updateSettingsRequest.settings(settings)
        elasticsearch7xClient.elasticsearchClient().indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT)

        indexNames.forEach { indexName ->
            def updateMappingRequest = new PutMappingRequest(indexName)
            updateMappingRequest.source(getDocumentMapping())
            elasticsearch7xClient.elasticsearchClient().indices().putMapping(updateMappingRequest, RequestOptions.DEFAULT)
        }
    }

    @Test
    void updateIndexSettings8xTest() {
        String newIndexName = "${Document.INDEX_NAME}${getUniqueSuffix()}"
        this.createIndex8x(newIndexName, getIndexSettings(), getDocumentMapping())
        List<String> indexNames = [newIndexName]

        def settingsResponse = elasticsearch8xClient.elasticsearchClient().indices().getSettings({ builder ->
            builder.index(*indexNames)
        })
        def currentLimit = settingsResponse.result().get(indexNames.first())
                .settings().index().mapping().totalFields().limit()

        elasticsearch8xClient.elasticsearchClient().indices().putSettings({ builder ->
            builder.index(*indexNames)
                    .settings({ settingBuilder ->
                        settingBuilder.mapping({ mappingBuilder ->
                            mappingBuilder.totalFields({ tfBuilder ->
                                tfBuilder.limit(currentLimit * 2)
                            })
                        })
                    })
        })

        indexNames.forEach { indexName ->
            elasticsearch8xClient.elasticsearchClient().indices().putMapping({ builder ->
                builder.index(indexName)
                        .withJson(new StringReader(JsonData.of(getDocumentMapping(), elasticsearch8xClient.elasticsearchJacksonJsonpMapper()).toJson().toString()))
            })
        }
    }

    @Test
    void deleteIndex7xTest() {
        String newIndexName = "${Document.INDEX_NAME}${getUniqueSuffix()}"
        this.createIndex7x(newIndexName, getIndexSettings(), getDocumentMapping())

        DeleteIndexRequest request = new DeleteIndexRequest(newIndexName)
        elasticsearch7xClient.elasticsearchClient().indices().delete(request, RequestOptions.DEFAULT)
    }

    @Test
    void deleteIndex8xTest() {
        String newIndexName = "${Document.INDEX_NAME}${getUniqueSuffix()}"
        this.createIndex8x(newIndexName, getIndexSettings(), getDocumentMapping())

        elasticsearch8xClient.elasticsearchClient().indices().delete({ builder ->
            builder.index(newIndexName)
        })
    }

    @Test
    void getMapping7xTest() {
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"

        this.createIndex7x(indexName, getIndexSettings(), getDocumentMapping())

        GetMappingsRequest request = new GetMappingsRequest()
        request.indices(indexName)

        GetMappingsResponse response = elasticsearch7xClient.elasticsearchClient()
                .indices().getMapping(request, RequestOptions.DEFAULT)

        def allMappings = response.mappings()

        allMappings.keySet().each { String index ->
            def mappingProperties = allMappings[index].sourceAsMap().properties

            // Go through all the mappings, and add them to the filterCriteria
            mappingProperties.each { String fieldName, Map properties ->
                // for field name with nested property
                def isNestedField = properties.type == "nested"
                if (isNestedField) {
                    def nestedProperties = properties.properties
                }
            }
        }
    }

    @Test
    void getMapping8xTest() {
        String uniqueSuffix = getUniqueSuffix()
        String indexName = "doc_${uniqueSuffix}"

        this.createIndex8x(indexName, getIndexSettings(), getDocumentMapping())

        GetMappingRequest.Builder requestBuilder = new GetMappingRequest.Builder()
        requestBuilder.index(indexName)
        GetMappingResponse response = elasticsearch8xClient.elasticsearchClient()
                .indices().getMapping(requestBuilder.build())

        def allMappings = response.result()

        allMappings.keySet().each { String index ->
            def mappingProperties = allMappings[index].mappings().properties()

            mappingProperties.each { String fieldName, Property property ->
                // for field name with nested property
                if (property._kind() == Property.Kind.Nested) {
                    def nestedProperty = property.nested().properties()
                }
            }
        }

    }
}

