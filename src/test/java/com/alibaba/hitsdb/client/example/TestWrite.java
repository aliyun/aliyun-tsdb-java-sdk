package com.alibaba.hitsdb.client.example;

import java.io.IOException;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.value.request.Point;

public class TestWrite {
	public static void main(String[] args) throws InterruptedException, IOException {
//		example.hitsdb.com
//		11.229.156.86
		HiTSDBConfig config = HiTSDBConfig.address("100.81.154.171", 8242).config();

		HiTSDB tsdb = HiTSDBClientFactory.connect(config);

		// 构造数据并写入HiTSDB
		for (int i = 0; i < 1; i++) {
			Point point = Point.metric("test").tag("V", "1.0").value(System.currentTimeMillis(), 123.4567).build();
			Thread.sleep(1000);  // 1秒提交1次
			tsdb.put(point);
		}

		// 安全关闭客户端，以防数据丢失。
		System.out.println("关闭");
		tsdb.close();
	}
}