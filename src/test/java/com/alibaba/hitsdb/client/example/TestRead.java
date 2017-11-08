package com.alibaba.hitsdb.client.example;

import java.io.IOException;
import java.util.List;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;

public class TestRead {
	public static void main(String[] args) throws IOException {
		HiTSDBConfig config = HiTSDBConfig.address("100.81.154.171", 8242).config();
		HiTSDB tsdb = HiTSDBClientFactory.connect(config);

		// 构造查询条件并查询数据。
		long now = System.currentTimeMillis();

		// 查询一小时的数据
		Query query = Query.timeRange(now - 3600 * 1000, now)
				.sub(SubQuery.metric("test").aggregator(Aggregator.NONE).tag("V", "1.0").build()).build();

		// 查询数据
		List<QueryResult> result = tsdb.query(query);

		// 打印输出
		System.out.println(result);

		// 安全关闭客户端，以防数据丢失。
		tsdb.close();
	}
}
