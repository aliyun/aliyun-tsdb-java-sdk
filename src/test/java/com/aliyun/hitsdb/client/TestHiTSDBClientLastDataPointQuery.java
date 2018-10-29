package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.LastPointQuery;
import com.aliyun.hitsdb.client.value.request.LastPointSubQuery;
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

@Ignore
public class TestHiTSDBClientLastDataPointQuery {
    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .config();
        
        tsdb = HiTSDBClientFactory.connect(config);
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
        Map<String,String> tags = new HashMap<String, String>();
        tags.put("uid","1");
        tags.put("id","6");
        LastPointQuery query = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .timestamp(1537520409729l)
                .sub(LastPointSubQuery.builder("test.1",tags).build()).build();
        System.out.println(query.toJSON());
        List<LastDataValue> lastDataValues = tsdb.queryLast(query);

        System.out.println(lastDataValues);
    }

    @Test
    public void testTsuidLast() {
        List<String> tsuids = new ArrayList<String>();
        tsuids.add("10000B7C0000950000810000FF00006F");
        LastPointQuery query = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .timestamp(1537520409729l)
                .sub(LastPointSubQuery.builder(tsuids).build()).build();

        System.out.println(query.toJSON());
        List<LastDataValue> lastDataValues = tsdb.queryLast(query);

        System.out.println(lastDataValues);
    }
    
}
