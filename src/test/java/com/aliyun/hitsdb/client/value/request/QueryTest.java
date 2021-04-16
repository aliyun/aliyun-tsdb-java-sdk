package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.QueryType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class QueryTest {
    @Test
    public void testMultiFieldQueryTestSerialization() {
        {
            MultiFieldQuery query = MultiFieldQuery
                    .start(4294968)
                    .queryType(QueryType.ALL)
                    .showType()
                    .sub(MultiFieldSubQuery.metric("wind1")
                            .fieldsInfo(MultiFieldSubQueryDetails.field("*").aggregator(Aggregator.NONE).build())
                            .build())
                    .build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"queries\":[{\"fields\":[{\"aggregator\":\"none\",\"aggregatorType\":\"NONE\",\"delta\":false,\"field\":\"*\",\"rate\":false,\"top\":0}],\"index\":0,\"metric\":\"wind1\"}],\"start\":4294968,\"type\":\"ALL\"}", serializedString);
        }
        {
            MultiFieldQuery query = MultiFieldQuery
                    .start(4294968)
                    .showType()
                    .sub(MultiFieldSubQuery.metric("wind1")
                            .fieldsInfo(MultiFieldSubQueryDetails.field("*").aggregator(Aggregator.NONE).build())
                            .build())
                    .build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"queries\":[{\"fields\":[{\"aggregator\":\"none\",\"aggregatorType\":\"NONE\",\"delta\":false,\"field\":\"*\",\"rate\":false,\"top\":0}],\"index\":0,\"metric\":\"wind1\"}],\"start\":4294968}", serializedString);
        }
    }

    @Test
    public void testQueryTestSerialization() {
        {
            Query query = Query
                    .start(4294968)
                    .queryType(QueryType.ALL)
                    .showType()
                    .sub(SubQuery.metric("metric").aggregator(Aggregator.NONE)
                            .tag(new HashMap<String, String>()).build()).build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"queries\":[{\"aggregator\":\"none\",\"index\":0,\"metric\":\"metric\"}],\"start\":4294968,\"type\":\"ALL\"}", serializedString);
        }
        {
            Query query = Query
                    .start(4294968)
                    .showType()
                    .sub(SubQuery.metric("metric").aggregator(Aggregator.NONE)
                            .tag(new HashMap<String, String>()).build()).build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"queries\":[{\"aggregator\":\"none\",\"index\":0,\"metric\":\"metric\"}],\"start\":4294968}", serializedString);
        }
    }
}
