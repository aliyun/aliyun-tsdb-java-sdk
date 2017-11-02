package com.alibaba.hitsdb.client;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.callback.QueryCallback;
import com.alibaba.hitsdb.client.exception.VIPClientException;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.value.request.Query;
import com.alibaba.hitsdb.client.value.request.SubQuery;
import com.alibaba.hitsdb.client.value.response.QueryResult;
import com.alibaba.hitsdb.client.value.type.Aggregator;

public class TestHiTSDBClientQueryStringValue {

    HiTSDB tsdb;

    @Before
    public void init() throws VIPClientException, HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1")
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
        Date now = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, -1);
        Date startTime = calendar.getTime();

        Query query = Query
                .timeRange(startTime, now)
                .sub(SubQuery.metric("hello").aggregator(Aggregator.NONE).tag("tagk1", "tagv1").build())
                .build();
        
        List<QueryResult> result = tsdb.query(query);
        QueryResult queryResult = result.get(0);
        LinkedHashMap<Integer, String> sdps = queryResult.getSdps();
        LinkedHashMap<Integer, Number> dps = queryResult.getDps();
        System.out.println(sdps);
        System.out.println(dps);
        System.out.println("查询结果：" + result);
    }

    @Test
    public void testQueryCallback() {

        Query query = Query.timeRange(1501655667, 1501742067).sub(
                SubQuery.metric("mem.usage.GB").aggregator(Aggregator.AVG).tag("site", "et2").tag("appname", "hitsdb").build())
                .build();

        QueryCallback cb = new QueryCallback() {

            @Override
            public void response(String address, Query input, List<QueryResult> result) {
                System.out.println("查询参数：" + input);
                System.out.println("返回结果：" + result);
            }

            // 在需要处理异常的时候，重写failed方法
            @Override
            public void failed(String address, Query request, Exception ex) {
                super.failed(address, request, ex);
            }

        };

        tsdb.query(query, cb);

        try {
            Thread.sleep(100000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
