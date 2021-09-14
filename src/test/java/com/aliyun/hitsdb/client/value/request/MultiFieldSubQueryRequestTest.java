package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.DownsampleDataSource;
import org.junit.Assert;
import org.junit.Test;

public class MultiFieldSubQueryRequestTest {

    @Test
    public void testMultiFieldSubQueryRequest() {
        final String metric = "wind";
        final String field = "speed";

        MultiFieldSubQueryDetails fieldSubQueryDetails = MultiFieldSubQueryDetails.field(field).aggregator(Aggregator.NONE)
                .downsample("5s-sum")
                .build();
        MultiFieldSubQuery subQuery = MultiFieldSubQuery.metric(metric)
                .fieldsInfo(fieldSubQueryDetails)
                .downsampleDataSource(DownsampleDataSource.DOWNSAMPLE)
                .build();

        Assert.assertNotNull(subQuery);

        String json = subQuery.toJSON();
        JSONObject object = JSON.parseObject(json);

        Assert.assertEquals(metric, object.getString("metric"));
        Assert.assertEquals("DOWNSAMPLE", object.getString("downsampleDataSource"));

        Assert.assertNotNull(object.getJSONArray("fields"));
        Assert.assertEquals(1, object.getJSONArray("fields").size());

        System.out.println(json);
    }
}
