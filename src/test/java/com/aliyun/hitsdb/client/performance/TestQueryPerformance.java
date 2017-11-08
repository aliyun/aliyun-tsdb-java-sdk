package com.aliyun.hitsdb.client.performance;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;

public class TestQueryPerformance {
	HiTSDB tsdb;
	CountDownLatch cl;
	static int SIZE = 1;

	@Before
	public void init() throws HttpClientInitException {
		cl = new CountDownLatch(SIZE);
		HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 8242)
//				 .useVIPServer()
				.httpConnectionPool(64)
				.readonly()
				.httpConnectTimeout(8)
				.config();
		tsdb = HiTSDBClientFactory.connect(config);
	}

	@After
	public void after() throws InterruptedException {
		try {
			cl.await();
			System.out.println("关闭");
			tsdb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testQuery() throws InterruptedException {
		final Query query = Query.timeRange(1508379411 - 3600 * 25, 1508379441 - 3600 * 24)
				.sub(SubQuery.metric("test1").aggregator(Aggregator.NONE)
						.downsample("0all-max").tag("V", "1.0").build())
				.sub(SubQuery.metric("test2").aggregator(Aggregator.NONE)
						.downsample("0all-max").tag("V", "1.0").build())
				.build();
		
		for (int i = 0; i < SIZE; i++) {
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					for (int i = 0; i < 1000; i++) {
						try {
							long t0 = System.currentTimeMillis();
							List<QueryResult> result = tsdb.query(query);
							long t1 = System.currentTimeMillis();

							if (t1 - t0 < 1000) {
								 System.out.println("查询花费："+(t1-t0)/1000.0 + "秒，查询结果：" + result);
							} else {
								System.err.println("查询花费：" + (t1 - t0) / 1000.0 + "秒，查询结果：" + result);
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					
					cl.countDown();
				}
			}).start();
			
		}

	}

	public void testQueryCallback() {

		Query query = Query.timeRange(1501655667, 1501742067).sub(SubQuery.metric("mem.usage.GB")
				.aggregator(Aggregator.AVG).tag("site", "et2").tag("appname", "hitsdb").build()).build();

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
