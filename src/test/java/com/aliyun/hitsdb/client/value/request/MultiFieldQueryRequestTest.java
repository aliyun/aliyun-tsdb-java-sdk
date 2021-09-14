package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.DownsampleDataSource;
import org.junit.Assert;
import org.junit.Test;

public class MultiFieldQueryRequestTest {

    @Test
    public void testMultiFieldSubQueryRequest() {
        final String metric = "wind";
        final String field = "speed";
        long startTimestamp = System.currentTimeMillis();
        long endTimestamp = startTimestamp + 1;

        MultiFieldSubQueryDetails fieldSubQueryDetails = MultiFieldSubQueryDetails.field(field).aggregator(Aggregator.NONE)
                .downsample("5s-sum")
                .build();
        MultiFieldSubQuery subQuery = MultiFieldSubQuery.metric(metric)
                .fieldsInfo(fieldSubQueryDetails)
                .downsampleDataSource(DownsampleDataSource.DOWNSAMPLE)
                .build();
        MultiFieldQuery query = MultiFieldQuery.start(startTimestamp).end(endTimestamp).sub(subQuery).build();

        Assert.assertNotNull(query);
        String json = query.toJSON();
        JSONObject object = JSON.parseObject(json);

        Assert.assertEquals(startTimestamp, object.getLongValue("start"));
        Assert.assertEquals(endTimestamp, object.getLongValue("end"));

        Assert.assertNotNull(object.getJSONArray("queries"));
        Assert.assertEquals(1, object.getJSONArray("queries").size());
        System.out.println(json);
    }
}
