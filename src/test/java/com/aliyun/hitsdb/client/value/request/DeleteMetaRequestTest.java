package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class DeleteMetaRequestTest {

    @Test
    public void testDeleteMetaRequest() {
        DeleteMetaRequest deleteMetaRequest = DeleteMetaRequest.metric("testMetric")
                .tag("tagk1", "tagv1").tag("tagk2", "tagv2")
                .deleteData(false).recursive(true)
                .build();
        Assert.assertNotNull(deleteMetaRequest);

        String json = deleteMetaRequest.toJSON();

        JSONObject object = JSON.parseObject(json);

        Assert.assertTrue("testMetric".equals(object.getString("metric")));
        Assert.assertFalse(object.getBoolean("deleteData"));
        Assert.assertTrue(object.getBoolean("recursive"));
        Assert.assertNull(object.getJSONObject("fields"));

        System.out.println(json);
    }
}
