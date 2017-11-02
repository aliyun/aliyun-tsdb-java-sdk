package com.alibaba.hitsdb.client;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.exception.VIPClientException;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.value.request.Query;
import com.alibaba.hitsdb.client.value.request.SubQuery;
import com.alibaba.hitsdb.client.value.response.QueryResult;
import com.alibaba.hitsdb.client.value.type.Aggregator;

public class TestHiTSDBClientLast {
    HiTSDB tsdb;

    @Before
    public void init() throws VIPClientException, HttpClientInitException {
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
    public void testLast() {
        Date now = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date startTime = calendar.getTime();

        Query query = Query.timeRange(startTime, now).sub(
                SubQuery.metric("test1")
                .aggregator(Aggregator.AVG)
                .tag("tagk1", "tagv1").build()
        ).build();

        List<QueryResult> result = tsdb.last(query, 10);
        System.out.println(result.get(0).getDps().size());
        System.out.println("查询结果：" + result);
    }
    
}
