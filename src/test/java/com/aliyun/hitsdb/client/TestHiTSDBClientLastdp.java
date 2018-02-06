package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Timeline;
import com.aliyun.hitsdb.client.value.response.LastDPValue;

public class TestHiTSDBClientLastdp {
	HiTSDB tsdb;

	@Before
	public void init() throws HttpClientInitException {
		HiTSDBConfig config = HiTSDBConfig.address("test.host", 3242).config();
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
	public void testLastdp() throws InterruptedException {
		Date now = new Date();
		System.out.println("当前时间:" + now.getTime());
		tsdb.put(Point.metric("metric-1").tag("tk1", "tv1").value(now, 123.4).build(),
				Point.metric("metric-2").tag("tk2", "tv2").value(now, 567.8).build(),
				Point.metric("metric-3").tag("tk3", "tv3").value(now, 910.11).build());

		Thread.sleep(3000);
		Timeline tl0 = Timeline.metric("metric-1").tag("tk1", "tv1").build();
		List<LastDPValue> result0 = tsdb.lastdp(tl0);
		System.out.println("last:" + result0);

		Timeline tl1 = Timeline.metric("metric-2").tag("tk2", "tv2").build();
		List<LastDPValue> result1 = tsdb.lastdp(tl1);
		System.out.println("last:" + result1);

		Timeline tl2 = Timeline.metric("metric-3").tag("tk3", "tv3").build();
		List<LastDPValue> result2 = tsdb.lastdp(tl2);
		System.out.println("last:" + result2);
	}
}
