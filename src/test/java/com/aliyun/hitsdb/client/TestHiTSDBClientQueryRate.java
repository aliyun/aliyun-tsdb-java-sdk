package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.RateOptions;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestHiTSDBClientQueryRate {

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig
    		        .address("localhost",8242)
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
    public void testQuery() {
        long end = System.currentTimeMillis();
        long start = end - 5 * 60 * 1000;
        Query query = Query
                .timeRange(start, end)
                .msResolution(true)
                .sub(SubQuery.metric("metric").aggregator(Aggregator.SUM)
                        .rate()
                        .downsample("1m-avg")
                        .tag("tagk1","tagv1")
                        .build())
                .build();

        System.out.println(query.toJSON());

        try {
            List<QueryResult> result = tsdb.query(query);
            System.out.println("查询结果：" + result.size());
            System.out.println("查询结果：" + result);
        } catch (HttpUnknowStatusException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQueryRateOptions() {
        long end = System.currentTimeMillis();
        long start = end - 5 * 60 * 1000;
        Query query = Query
                .timeRange(start, end)
                .msResolution(true)
                .sub(SubQuery.metric("metric").aggregator(Aggregator.SUM)
                        .rate(RateOptions.newBuilder().counter(false).dropResets(true).build())
                        .downsample("1m-avg")
                        .tag("tagk1","tagv1")
                        .build())
                .build();

        System.out.println(query.toJSON());

        try {
            List<QueryResult> result = tsdb.query(query);
            System.out.println("查询结果：" + result.size());
            System.out.println("查询结果：" + result);
        } catch (HttpUnknowStatusException e) {
            e.printStackTrace();
        }
    }

}
