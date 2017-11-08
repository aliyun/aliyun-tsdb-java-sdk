package com.aliyun.hitsdb.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.Timeline;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.response.TagResult;
import com.aliyun.hitsdb.client.value.type.Suggest;

public interface HiTSDB extends Closeable {
	/**
	 * @param point
	 */
	void put(Point point);

	/**
	 * @param point
	 */
	Result putSync(Collection<Point> points);

	/**
	 * @param point
	 */
	<T extends Result> T putSync(Collection<Point> points, Class<T> resultType);

	/**
	 * @param query
	 * @param callback
	 */
	void query(Query query, QueryCallback callback);

	/**
	 * @param query
	 * @return
	 */
	List<QueryResult> query(Query query) throws HttpUnknowStatusException;

	/**
	 * @param query
	 * @param num
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	List<QueryResult> last(Query query, int num) throws HttpUnknowStatusException;

	/**
	 * @param metric
	 * @param start
	 * @param end
	 */
	void deleteData(String metric, long startTime, long endTime) throws HttpUnknowStatusException;

	/**
	 * @param metric
	 * @param startDate
	 * @param endDate
	 */
	void deleteData(String metric, Date startDate, Date endDate) throws HttpUnknowStatusException;

	/**
	 * @param metric
	 * @param tags
	 */
	void deleteMeta(String metric, Map<String, String> tags) throws HttpUnknowStatusException;

	/**
	 * @param timeline
	 */
	void deleteMeta(Timeline timeline) throws HttpUnknowStatusException;

	/**
	 * @param lifetime
	 */
	void ttl(int lifetime) throws HttpUnknowStatusException;

	/**
	 * @param lifetime
	 * @param unit
	 */
	void ttl(int lifetime, TimeUnit unit) throws HttpUnknowStatusException;

	/**
	 * @return
	 */
	int ttl() throws HttpUnknowStatusException;

	/**
	 * @param type
	 * @param prefix
	 * @param max
	 * @return
	 */
	List<String> suggest(Suggest type, String prefix, int max) throws HttpUnknowStatusException;

	/**
	 * @param tagkey
	 * @param tagValuePrefix
	 * @param max
	 * @return
	 */
	List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException;

	/**
	 * 关闭客户端连接。<br>
	 * 
	 * @param force
	 *            若为 true 表示强制关闭，为 false 表示优雅关闭
	 * @throws IOException
	 */
	void close(boolean force) throws IOException;
}
