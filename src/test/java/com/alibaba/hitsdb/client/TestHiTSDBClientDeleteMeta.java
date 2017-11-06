package com.alibaba.hitsdb.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.value.request.Timeline;

public class TestHiTSDBClientDeleteMeta {
    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        tsdb = HiTSDBClientFactory.connect("127.0.0.1", 8242);
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
    public void testDeleteMeta1() {
        Timeline timeline = Timeline.metric("hello").tag("tagk1", "tagv1").build();
        tsdb.deleteMeta(timeline);
    }

    @Test
    public void testDeleteMeta2() {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("tagk1", "tagv1");
        tsdb.deleteMeta("hello", tags);
    }

}
