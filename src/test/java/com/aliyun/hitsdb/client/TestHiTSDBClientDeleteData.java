package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.*;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.util.DateUtils;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class TestHiTSDBClientDeleteData {
    HiTSDB tsdb;
    
    @Before
    public void init() throws HttpClientInitException, InterruptedException {
        tsdb = HiTSDBClientFactory.connect("127.0.0.1", 8242);

        Date now = DateUtils.now();

        Date ts = DateUtils.add(now, - TimeUnit.DAYS.toMillis(5));
        Random random = new Random();

        int i;
        for (i = 0; i < 3; i++) {
            double radval = random.nextDouble() * 100;
            Point point = Point
                    .metric("hello")
                    .tag("tag1", "val1")
                    .tag("tag2", "val2")
                    .timestamp(ts.getTime()+ i * 1000)
                    .value(radval)
                    .build();
            tsdb.putSync(point);
        }

        for (i = 0; i < 3; i++) {
            double radval = random.nextDouble() * 100;
            Point point = Point
                    .metric("hello")
                    .tag("tag3", "val3")
                    .tag("tag4", "val4")
                    .timestamp(ts.getTime() + i * 1000)
                    .value(radval)
                    .build();
            tsdb.putSync(point);
        }
        Thread.sleep(5000);
    }

    @After
    public void after() {
        try {
            Date now = DateUtils.now();
            Date start = DateUtils.add(now, - TimeUnit.DAYS.toMillis(10));
            tsdb.deleteData("hello", start, now);
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testDeleteDataWithTags() {
        Date now = DateUtils.now();
        Date start = DateUtils.add(now, - TimeUnit.DAYS.toMillis(10));
        HashMap<String, String> tags = new HashMap<String, String>();
        tags.put("tag1", "val1");
        tags.put("tag2", "val2");
        tsdb.deleteData("hello", tags, start, now);

        Query query = Query
                .timeRange(start.getTime(), now.getTime())
                .msResolution(true)
                .sub(SubQuery.metric("hello")
                        .aggregator(Aggregator.NONE)
                        .tag(tags)
                        .build())
                .build();
        List<QueryResult> result = tsdb.query(query);
        Assert.assertTrue(result.isEmpty());

        HashMap<String, String> restTags = new HashMap<String, String>();
        restTags.put("tag3", "val3");
        restTags.put("tag4", "val4");
        query = Query
                .timeRange(start.getTime(), now.getTime())
                .msResolution(true)
                .sub(SubQuery.metric("hello")
                        .aggregator(Aggregator.NONE)
                        .tag(restTags)
                        .build())
                .build();
        result = tsdb.query(query);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).getDps().size());

        restTags.clear();
        restTags.put("tag3", "val3");
        query = Query
                .timeRange(start.getTime(), now.getTime())
                .msResolution(true)
                .sub(SubQuery.metric("hello")
                        .aggregator(Aggregator.NONE)
                        .tag(restTags)
                        .build())
                .build();
        result = tsdb.query(query);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).getDps().size());
    }

    @Test
    public void testDeleteDataWithPartialTag() {
        Date now = DateUtils.now();
        Date start = DateUtils.add(now, -TimeUnit.DAYS.toMillis(10));
        HashMap<String, String> tags = new HashMap<String, String>();
        tags.put("tag1", "val1");
        tsdb.deleteData("hello", tags, start, now);

        tags.put("tag2", "val2");
        Query query = Query
                .timeRange(start.getTime(), now.getTime())
                .msResolution(true)
                .sub(SubQuery.metric("hello")
                        .aggregator(Aggregator.NONE)
                        .tag(tags)
                        .build())
                .build();
        List<QueryResult> result = tsdb.query(query);
        Assert.assertTrue(result.isEmpty());
    }
    
    @Test(expected = com.aliyun.hitsdb.client.exception.http.HttpServerNotSupportException.class)
    public void testDeleteDataForTime() {
        Date now = DateUtils.now();
        Date start = DateUtils.add(now, - TimeUnit.DAYS.toMillis(10));
        int nowTime = DateUtils.toTimestampSecond(now);
        int startTime = DateUtils.toTimestampSecond(start);
        tsdb.deleteData("hello", nowTime, startTime);
    }
    
}
