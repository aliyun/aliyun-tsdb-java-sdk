package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.LastPointQuery;
import com.aliyun.hitsdb.client.value.request.LastPointSubQuery;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.LastDataValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Ignore
public class TestHiTSDBClientLastDataPointQuery {
    HiTSDB tsdb;

    public static final String metric = "test.1";
    public static final Map<String, String> tags = new HashMap<String, String>();

    public static final long timestamp = 1537520409729l;

    static {
        tags.put("uid", "1");
        tags.put("id", "6");
    }

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .config();

        tsdb = HiTSDBClientFactory.connect(config);

        tsdb.putSync(Point.metric(metric).tag(tags).value(timestamp,Math.random()).build());
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
    public void testMetricLast() {
        LastPointQuery query = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .timestamp(timestamp)
                .sub(LastPointSubQuery.builder(metric, tags).build()).build();
        System.out.println(query.toJSON());
        List<LastDataValue> lastDataValues = tsdb.queryLast(query);

        System.out.println(lastDataValues);
        assertEquals(lastDataValues.size(),1);
    }

    @Test
    public void testMetricLast1() {

        LastPointQuery query = LastPointQuery
                .builder()
                .msResolution(true)
                .timestamp(timestamp)
                .sub(LastPointSubQuery.builder(metric, tags).build()).build();
        System.out.println(query.toJSON());
        List<LastDataValue> lastDataValues = tsdb.queryLast(query);

        System.out.println(lastDataValues);
        assertEquals(lastDataValues.size(),1);
    }

    @Test
    public void testMetricLast2() {

        LastPointQuery query = LastPointQuery
                .builder()
                .timestamp(timestamp)
                .sub(LastPointSubQuery.builder(metric, tags).build()).build();
        System.out.println(query.toJSON());
        List<LastDataValue> lastDataValues = tsdb.queryLast(query);

        System.out.println(lastDataValues);
        assertEquals(lastDataValues.size(),1);
    }


    @Test
    public void testMetricLast3() {

        LastPointQuery query = LastPointQuery
                .builder()
                .sub(LastPointSubQuery.builder(metric, tags).build()).build();
        System.out.println(query.toJSON());
        List<LastDataValue> lastDataValues = tsdb.queryLast(query);

        System.out.println(lastDataValues);
        assertEquals(lastDataValues.size(),1);
    }

}
