package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.request.Filter;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.FilterType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHiTSDBClientQueryReverse {

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
        long current = System.currentTimeMillis() / 1000;
        long start = current;
        int size = 100;
        String metric = "test-test-metric";
        Map<String,String> tags = new HashMap<String, String>();
        tags.put("tagKey","tagValue");
        List<Point> points = new ArrayList<Point>(size);
        for(int i = 0;i < size;i ++){
            Point point = Point.metric(metric)
                    .tag(tags)
                    .value(start,i).build();
            start ++;
            points.add(point);
        }
        tsdb.putSync(points);
//
        Query query = Query
                .timeRange(current - 1000,start + 1000)
                .sub(SubQuery.metric(metric).aggregator(Aggregator.NONE).tag(tags).build())
                .build();

        try {
            List<QueryResult> result = tsdb.query(query);
            for(QueryResult queryResult : result){
                System.out.println(queryResult.getDps());
                System.out.println("-------------");
                System.out.println(queryResult.getOrderDps());
                System.out.println("-------------");
                System.out.println(queryResult.getOrderDps(true));
            }
        } catch (HttpUnknowStatusException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQueryCallback() {
            long current = System.currentTimeMillis() / 1000;
            long start = current;
            int size = 100;
            String metric = "test-test-metric";
            Map<String,String> tags = new HashMap<String, String>();
            tags.put("tagKey","tagValue");
            List<Point> points = new ArrayList<Point>(size);
            for(int i = 0;i < size;i ++){
                Point point = Point.metric(metric)
                        .tag(tags)
                        .value(start,i).build();
                start ++;
                points.add(point);
            }
            tsdb.putSync(points);
//
            Query query = Query
                    .timeRange(current - 1000,start + 1000)
                    .sub(SubQuery.metric(metric).aggregator(Aggregator.NONE).tag(tags).build())
                    .build();

        QueryCallback cb = new QueryCallback() {

            @Override
            public void response(String address, Query input, List<QueryResult> result) {
                System.out.println("查询参数：" + input);
                System.out.println("返回结果：" + result);
                for(QueryResult queryResult : result){
                    System.out.println(queryResult.getDps());
                    System.out.println("-------------");
                    System.out.println(queryResult.getOrderDps());
                    System.out.println("-------------");
                    System.out.println(queryResult.getOrderDps(true));
                }
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
