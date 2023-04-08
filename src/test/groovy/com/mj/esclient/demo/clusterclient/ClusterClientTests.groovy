package com.mj.esclient.demo.clusterclient

import co.elastic.clients.elasticsearch.cluster.GetClusterSettingsRequest
import co.elastic.clients.elasticsearch.cluster.GetClusterSettingsResponse
import com.mj.esclient.demo.BaseTest
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse
import org.elasticsearch.client.RequestOptions
import org.junit.jupiter.api.Test

class ClusterClientTests extends BaseTest{
    @Test
    void getClusterSetting7xTest(){
        ClusterGetSettingsRequest request = new ClusterGetSettingsRequest().includeDefaults(true)
        ClusterGetSettingsResponse response = elasticsearch7xClient.elasticsearchClusterClient().getSettings(request, RequestOptions.DEFAULT)
        String maxContentLengthStr = response.getSetting("http.max_content_length")
        assert maxContentLengthStr == "100mb"
    }

    @Test
    void getClusterSetting8xTest(){
        GetClusterSettingsRequest request = new GetClusterSettingsRequest.Builder().includeDefaults(true).build()
        GetClusterSettingsResponse response = elasticsearch8xClient.elasticsearchClusterClient().getSettings(request)
        String maxContentLengthStr = response.defaults().get("http").toJson().asJsonObject().getString("max_content_length")
        assert maxContentLengthStr == "100mb"
    }

}
