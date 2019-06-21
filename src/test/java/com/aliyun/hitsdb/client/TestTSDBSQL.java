package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.SQLResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class TestTSDBSQL {

	private TSDB tsdb;

	long startTime;

	@Before
	public void setup() {
		TSDBConfig config = TSDBConfig.address("127.0.0.1", 8242)
				.httpConnectTimeout(90)
				// 批次写入时每次提交数据量
				.batchPutSize(500)
				// 单值异步写入线程数
				.batchPutConsumerThreadCount(1)
				// 多值异步写入缓存队列长度
				.multiFieldBatchPutBufferSize(10000)
				// 多值异步写入线程数
				.multiFieldBatchPutConsumerThreadCount(1)
				.config();

		// 特别注意，TSDB只需初始化一次即可
		tsdb = TSDBClientFactory.connect(config);

		startTime = System.currentTimeMillis() - 2000;
		tsdb.putSync(Point.metric("test-test").tag("tk1", "tv1").value(startTime, Math.random()).build());
	}

	@After
	public void tearDown() throws IOException {
		tsdb.close();
	}


	@Test
	public void test_show_tables() throws IOException {
		SQLResult sqlResult = tsdb.queryBySQL("SHOW TABLES FROM TSDB");
		assertNotNull(sqlResult);
	}

	@Test(expected = HttpUnknowStatusException.class)
	public void test_show_tables_with_exception() throws IOException {
		SQLResult sqlResult = tsdb.queryBySQL("SHOW TABLES FROM TSD");
		assertNotNull(sqlResult);
	}

	@Test
	public void test_describe_table() throws IOException {
		SQLResult sqlResult = tsdb.queryBySQL("DESCRIBE TSDB.`test-test`");
		assertNotNull(sqlResult);
	}

	@Test(expected = HttpUnknowStatusException.class)
	public void test_describe_table_with_exception() throws IOException {
		SQLResult sqlResult = tsdb.queryBySQL("DESCRIBE TSDB.`test`");
		assertNotNull(sqlResult);
	}


	@Test
	public void test_query_by_sql() throws IOException {
		String sql = "SELECT `timestamp`, `value` from tsdb.`test-test` where `timestamp` >= " + (startTime - 8 * 60 * 60 * 1000);
		SQLResult sqlResult = tsdb.queryBySQL(sql);
		assertNotNull(sqlResult);
	}

}
