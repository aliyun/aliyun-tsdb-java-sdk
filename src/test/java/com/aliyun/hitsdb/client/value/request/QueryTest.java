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
        {
            MultiFieldQuery query = MultiFieldQuery
                    .start(4294968)
                    .slimit(1)
                    .limit(2)
                    .offset(3)
                    .rlimit(4)
                    .roffset(5)
                    .sub(MultiFieldSubQuery.metric("wind1")
                            .fieldsInfo(MultiFieldSubQueryDetails.field("*").aggregator(Aggregator.NONE).build())
                            .build())
                    .build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"limit\":2,\"offset\":3,\"queries\":[{\"fields\":[{\"aggregator\":\"none\",\"aggregatorType\":\"NONE\",\"delta\":false,\"field\":\"*\",\"rate\":false,\"top\":0}],\"index\":0,\"metric\":\"wind1\"}],\"rlimit\":4,\"roffset\":5,\"slimit\":1,\"start\":4294968}", serializedString);
        }
        {
            MultiFieldQuery query = MultiFieldQuery
                    .start(4294968)
                    .slimit(1)
                    .limit(2)
                    .offset(3)
                    .rlimit(4)
                    .roffset(5)
                    .sub(MultiFieldSubQuery.metric("wind1")
                            .slimit(6)
                            .limit(7)
                            .offset(8)
                            .rlimit(9)
                            .roffset(10)
                            .fieldsInfo(MultiFieldSubQueryDetails.field("*").aggregator(Aggregator.NONE).build())
                            .build())
                    .build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"limit\":2,\"offset\":3,\"queries\":[{\"fields\":[{\"aggregator\":\"none\",\"aggregatorType\":\"NONE\",\"delta\":false,\"field\":\"*\",\"rate\":false,\"top\":0}],\"index\":0,\"limit\":7,\"metric\":\"wind1\",\"offset\":8,\"rlimit\":9,\"roffset\":10,\"slimit\":6}],\"rlimit\":4,\"roffset\":5,\"slimit\":1,\"start\":4294968}", serializedString);
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
        {
            Query query = Query
                    .start(4294968)
                    .showType()
                    .sub(SubQuery.metric("metric").aggregator(Aggregator.NONE)
                            .tag(new HashMap<String, String>()).slimit(2).build()).build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"queries\":[{\"aggregator\":\"none\",\"index\":0,\"metric\":\"metric\",\"slimit\":2}],\"start\":4294968}", serializedString);
        }

        {
            Query query = Query
                    .start(4294968)
                    .slimit(1)
                    .limit(2)
                    .offset(3)
                    .sub(SubQuery.metric("metric").aggregator(Aggregator.NONE)
                            .tag(new HashMap<String, String>()).slimit(2).build()).build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"limit\":2,\"offset\":3,\"queries\":[{\"aggregator\":\"none\",\"index\":0,\"metric\":\"metric\",\"slimit\":2}],\"slimit\":1,\"start\":4294968}", serializedString);
        }

        {
            Query query = Query
                    .start(4294968)
                    .slimit(1)
                    .limit(2)
                    .offset(3)
                    .sub(SubQuery.metric("metric").aggregator(Aggregator.NONE)
                            .slimit(4)
                            .limit(5)
                            .offset(6)
                            .tag(new HashMap<String, String>()).build()).build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"limit\":2,\"offset\":3,\"queries\":[{\"aggregator\":\"none\",\"index\":0,\"limit\":5,\"metric\":\"metric\",\"offset\":6,\"slimit\":4}],\"slimit\":1,\"start\":4294968}", serializedString);
        }
    }
}
