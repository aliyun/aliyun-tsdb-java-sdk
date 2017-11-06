package com.alibaba.hitsdb.client.compress;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.HiTSDB;
import com.alibaba.hitsdb.client.HiTSDBClientFactory;
import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.callback.QueryCallback;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.value.request.Query;
import com.alibaba.hitsdb.client.value.request.SubQuery;
import com.alibaba.hitsdb.client.value.response.QueryResult;
import com.alibaba.hitsdb.client.value.type.Aggregator;

public class TestHiTSDBClientQuery {

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .httpCompress(true)
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
        calendar.add(Calendar.DATE, -1);
        Date startTime = calendar.getTime();

        Query query = Query.timeRange(startTime, now).sub(
                SubQuery.metric("test1")
                .aggregator(Aggregator.AVG)
                .tag("tagk1", "tagv1").build()
        ).build();

        List<QueryResult> result = tsdb.query(query);
        System.out.println("查询结果：" + result);
    }

    @Test
    public void testQueryCallback() {

        Query query = Query.timeRange(1501655667, 1501742067)
        		.sub(SubQuery.metric("mem.usage.GB").aggregator(Aggregator.AVG).tag("site", "et2").tag("appname", "hitsdb").build())
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

    }

}
