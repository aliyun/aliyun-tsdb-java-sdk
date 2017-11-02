package com.alibaba.hitsdb.client.performance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.hitsdb.client.value.request.Point;

@Ignore
public class TestFastJSON {
	int BatchTimes = 50000;
	int BatchSize = 1000;
	int P_NUM = 1;
	final CountDownLatch countDownLatch = new CountDownLatch(P_NUM);
	final AtomicLong T0 = new AtomicLong();
	final AtomicLong T1 = new AtomicLong();

	@Test
	public void test() throws InterruptedException, IOException {
		System.out.println("按下任意键，开始运行...");
		while (true) {
			int read = System.in.read();
			if (read != 0) {
				break;
			}
		}

		for (int thread_index = 0; thread_index < P_NUM; thread_index++) {
			new Thread(new Runnable() {

				@Override
				public void run() {
					T0.compareAndSet(0, System.currentTimeMillis());
					long t0 = System.currentTimeMillis();
					for (int n = 0; n < BatchTimes; n++) {
						List<Point> ps = new ArrayList<Point>(BatchSize);
						for (int i = 0; i < BatchSize; i++) {
							Point p = createPoint(1, 1.12345678910);
							ps.add(p);
						}
						JSON.toJSONString(ps);
					}
					long t1 = System.currentTimeMillis();

					double dt = t1 - t0;
					System.out.println(dt + "ms \n" + BatchTimes * BatchSize / dt + "K/s \n-----");
					System.out.println();

					countDownLatch.countDown();
				}
			}).start();
		}

		// 发送线程发送完毕
		countDownLatch.await();
		System.out.println("主线程将要结束，尝试优雅关闭");
	}

	@After
	public void end() throws IOException {
		// 优雅关闭
		T1.compareAndSet(0, System.currentTimeMillis());

		double dt = T1.get() - T0.get();
		System.out.println("时间：" + (dt));
		System.out.println("Fast解析速率" + BatchTimes * BatchSize * P_NUM / dt + "K/s");
		System.out.println("结束");
	}

	public Point createPoint(int tag, double value) {
		int t = (int) (System.currentTimeMillis() / 1000);
		return Point.metric("test-performance").tag("tag", String.valueOf(tag)).value(t, value).build();
	}
}
