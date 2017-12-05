package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;

public class TestHiTSDBClientSyncPut {
	
	HiTSDB tsdb;

	@Before
	public void init() throws HttpClientInitException {
		BatchPutCallback pcb = new BatchPutCallback() {

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

		HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 3242)
				.listenBatchPut(pcb).httpConnectTimeout(90).config();
		tsdb = HiTSDBClientFactory.connect(config);
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
	public void testPutData() {
		int t = (int) (1508742134297l / 1000); // 1508742134
		int t1 = t - 1;
		Point point = Point.metric("test-test-test").tag("level", "500").timestamp(t1).value(123.4567).build();
		Result result = tsdb.putSync(point);
		System.out.println(result);
	}
}
