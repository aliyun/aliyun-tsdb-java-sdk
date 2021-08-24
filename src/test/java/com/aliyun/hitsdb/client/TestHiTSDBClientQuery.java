package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;

import javax.print.attribute.standard.NumberUp;

import static org.junit.Assert.assertEquals;

public class TestHiTSDBClientQuery {

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig
                .address("localhost", 8242)
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
//        Date now = new Date();
//        Calendar calendar = Calendar.getInstance();
//        calendar.add(Calendar.DATE, -3);
//        Date startTime = calendar.getTime();
//	    	int t0 = (int) (1508742134297l/1000);
        long startTime = System.currentTimeMillis();
        double value = Math.random();
        Point point = Point.metric("test-test-test")
                .tag("K1","V1")
                .value(startTime,value)
                .build();
        tsdb.putSync(point);
        Query query = Query
                .timeRange(startTime - 100, startTime + 100)
                .msResolution(true)
                .sub(SubQuery.metric("test-test-test")
                        .aggregator(Aggregator.NONE)
                        .tag("K1","V1")
                        .hint(Collections.<String, Map<String, Integer>>emptyMap())
                        .build())
                .hint(Collections.<String, Map<String, Integer>>emptyMap())
                .build();
            List<QueryResult> result = tsdb.query(query);
            assertEquals(1,result.size());
            assertEquals(1,result.get(0).getDps().size());
           assertEquals(value,((Number)result.get(0).getDps().get(startTime)).doubleValue(),0.00000000001);

    }

    @Test
    public void testQueryWithShowType() {
        Query query = Query.timeRange(1346846400, 1346846401)
                .sub(SubQuery.metric("sys.cpu.nice2")
                        .aggregator(Aggregator.NONE)
                        .build())
                .showType()
                .build();

        List<QueryResult> result = tsdb.query(query);
        System.out.println("查询结果：" + result);
        final QueryResult queryResult = result.get(0);
        final Class<?> type = queryResult.getType();
        System.out.println(type);
    }

    @Test
    public void testQueryWithSpecialType() {
        Query query = Query.timeRange(1346846400, 1346846401)
                .sub(SubQuery.metric("sys.cpu.nice2")
                        .aggregator(Aggregator.NONE)
                        .build())
                .withType(Long.class)
                .build();

        List<QueryResult> result = tsdb.query(query);
        System.out.println("查询结果：" + result);
        final QueryResult queryResult = result.get(0);
        final Class<?> type = queryResult.getType();
        System.out.println(type);
    }

    @Test
    public void testQueryCallback() {

//	    	int t0 = (int) (1508742134297l/1000);
        int t1 = (int) (1508742134297l / 1000);
        int t0 = t1 - 1;
        Query query = Query.timeRange(t0, t1)
                .sub(SubQuery.metric("test-test-test").aggregator(Aggregator.AVG).tag("level", "500").build())
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



    @Test
    public void testWriteAndQueryWithoutTags() throws InterruptedException {
        long current = System.currentTimeMillis();
        Map<String, String> tag = new HashMap();
        tag.put("ts-ts-test-test-tag-key", "ts-ts-test-test-tag-value");
        String metric = "ts-ts-test-test-value";

        double v1 = Math.random();
        double v2 = Math.random();

        // write with tag
        tsdb.putSync(Point.metric(metric).tag(tag).value(current, v1).build());
        // write without tag
        tsdb.putSync(Point.metric(metric).value(current, v2).build());

        Thread.sleep(1000);

        // query without tag
        List<QueryResult> results = tsdb.query(Query.start(current).end(System.currentTimeMillis()).sub(SubQuery
                .aggregator(Aggregator.NONE).metric(metric).build()
        ).build());

        assertEquals(results.size(), 2);
        for (int i = 0; i < results.size(); i++) {
            double v = ((Number) results.get(i).getDps().entrySet().iterator().next().getValue()).doubleValue();
            if(results.get(i).getTags().isEmpty()) {
                assertEquals(v2,v,0.0000000000001);
            } else {
                assertEquals(v1,v,0.0000000000001);
            }
        }

        // query with tag
        results = tsdb.query(Query.start(current).end(System.currentTimeMillis()).sub(SubQuery
                .aggregator(Aggregator.NONE).metric(metric).tag(tag).build()
        ).build());

        assertEquals(results.size(), 1);
        for (int i = 0; i < results.size(); i++) {
            double v = ((Number) results.get(i).getDps().entrySet().iterator().next().getValue()).doubleValue();
            assertEquals(v1, v, 0.0000000000001);
        }
    }
}
