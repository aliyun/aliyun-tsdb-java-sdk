package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.List;

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

public class TestHiTSDBClientQuery {

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
    		HiTSDBConfig config = HiTSDBConfig
    		        .address("test.host",3242)
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
	    	int t1 = (int) (1508742134297l/1000);
	    int 	t0 = t1 - 100;
        Query query = Query
                .timeRange(t0, t1)
                .sub(SubQuery.metric("test-test-test").aggregator(Aggregator.NONE).build())
//                .sub(SubQuery.metric("test-test-test").aggregator(Aggregator.NONE).tag("b", "2").build())
                .build();
        
        try {
	    		List<QueryResult> result = tsdb.query(query);
	    		System.out.println("查询结果：" + result.size());
	    		System.out.println("查询结果：" + result);
        } catch (HttpUnknowStatusException e) {
    			e.printStackTrace();
        }
    }

    @Test
    public void testQueryCallback() {
    	
//	    	int t0 = (int) (1508742134297l/1000);
	    	int t1 = (int) (1508742134297l/1000);
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

}
