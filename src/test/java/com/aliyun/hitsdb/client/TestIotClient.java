package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.TagsAddResult;
import com.aliyun.hitsdb.client.value.response.TagsShowResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/*
 * This test can only run in super-tag mode
 */
public class TestIotClient {
    TSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        TSDBConfig config = TSDBConfig
                .address("localhost", 8242)
                .config();
        tsdb = IotClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTagsOperation() {
        long startTime = System.currentTimeMillis();
        double value = Math.random();
        MultiFieldPoint point = MultiFieldPoint.metric("test")
                .tag("_id","1")
                .field("field1",value)
                .timestamp(startTime)
                .build();
        tsdb.multiFieldPutSync(point);

        TagsAddResult tagsAddResult = ((IotClient)tsdb).tagsAdd(new TagsAddInfo.Builder("test").id("1").tag("tagk1", "tagv1").build());
        Assert.assertEquals(1, tagsAddResult.getSuccess());

        List<TagsShowResult> tagsShowResult = ((IotClient)tsdb).tagsShow(new TagsShowInfo("test","1"));
        Assert.assertEquals(1, tagsShowResult.size());

        TagsShowResult result = tagsShowResult.get(0);
        Assert.assertEquals("tagv1", result.getTags().get("tagk1"));

        ((IotClient)tsdb).tagsRemove(new TagsRemoveInfo.Builder("test","1").tag("tagk1", "tagv1").build());
        tagsShowResult = ((IotClient)tsdb).tagsShow(new TagsShowInfo("test","1"));
        Assert.assertEquals(0, tagsShowResult.size());

    }
}
