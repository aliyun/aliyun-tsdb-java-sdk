package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryResult;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class MultiFieldPointAsyncPutTest {


    TSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        MultiFieldBatchPutCallback pcb = new MultiFieldBatchPutCallback() {
            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<MultiFieldPoint> points, Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<MultiFieldPoint> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("已处理" + count + "个点");
            }

        };

        BatchPutCallback batchPutCallback = new BatchPutCallback() {
            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("已处理" + count + "个点");
            }

        };

        TSDBConfig config = TSDBConfig.address("127.0.0.1", 8242)
                .listenMultiFieldBatchPut(pcb)
                .listenBatchPut(batchPutCallback)
                .httpConnectTimeout(90)
                .batchPutConsumerThreadCount(1)
                .multiFieldBatchPutConsumerThreadCount(1)
                .config();
        tsdb = TSDBClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            System.out.println("将要关闭");
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMultiFieldPointAsyncPut() {
        int t = (int) (1508742134297l / 1000);  // 1508742134
        int t1 = t - 1;
        MultiFieldPoint point = MultiFieldPoint
                .metric("test-test-test")
                .tag("a", "1")
                .tag("b", "2")
                .timestamp(t1)
                .field("f1", Math.random())
                .field("f2", Math.random())
                .build();
        tsdb.multiFieldPut(point);
    }

    @Test
    public void testSinglePointAsyncPut() {
        int t = (int) (1508742134297l / 1000);  // 1508742134
        int t1 = t - 1;
        Point point = Point
                .metric("test-test-test-single")
                .tag("a", "1")
                .tag("b", "2")
                .timestamp(t1)
                .value(Math.random())
                .build();
        tsdb.put(point);
    }



    @Test
    public void testSinglePointQuery() throws InterruptedException {
        long t = System.currentTimeMillis();
        double v1 = Math.random();
        Point point = Point
                .metric("test-test-test-single")
                .tag("a", "1")
                .tag("b", "2")
                .timestamp(t)
                .value(v1)
                .build();
        tsdb.put(point);
        Thread.sleep(2000);

        Query query = Query.start(t)
                .sub(SubQuery.metric("test-test-test-single")
                        .aggregator(Aggregator.NONE)
                        .tag("a", "1")
                        .tag("b", "2")
                        .build())
                .build();
        List<QueryResult> queryResults = tsdb.query(query);
        assertNotNull(queryResults);
        assertEquals(1, queryResults.size());
        QueryResult result = queryResults.get(0);
        assertEquals(1, result.getDps().size());
        Map.Entry<Long,Object> entry = result.getDps().entrySet().iterator().next();
        assertEquals(t, entry.getKey().longValue());
        assertEquals(v1, ((Number)entry.getValue()).doubleValue());
    }


    @Test
    public void testMultiFieldPointQuery() throws InterruptedException {
        long t = System.currentTimeMillis();
        double v1 = Math.random();
        double v2 = Math.random();
        MultiFieldPoint point = MultiFieldPoint
                .metric("test-test-test")
                .tag("a", "1")
                .tag("b", "2")
                .timestamp(t)
                .field("f1", v1)
                .field("f2", v2)
                .build();
        tsdb.multiFieldPut(point);
        Thread.sleep(2000);

        MultiFieldQuery query = MultiFieldQuery.start(t)
                .sub(MultiFieldSubQuery.metric("test-test-test")
                        .tag("a", "1")
                        .tag("b", "2")
                        .fieldsInfo(MultiFieldSubQueryDetails.field("f1").aggregator(Aggregator.NONE).build())
                        .fieldsInfo(MultiFieldSubQueryDetails.field("f2").aggregator(Aggregator.NONE).build())
                        .build())
                .build();
        List<MultiFieldQueryResult> queryResults = tsdb.multiFieldQuery(query);
        assertNotNull(queryResults);
        assertEquals(1, queryResults.size());
        MultiFieldQueryResult result = queryResults.get(0);
        assertEquals(1, result.getValues().size());
        List<Object> values = result.getValues().get(0);
        assertEquals(t, values.get(0));
        assertEquals(v1, ((Number)values.get(1)).doubleValue());
        assertEquals(v2, ((Number)values.get(2)).doubleValue());
    }

}
