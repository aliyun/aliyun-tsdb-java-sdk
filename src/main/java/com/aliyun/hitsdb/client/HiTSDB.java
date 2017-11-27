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
import com.aliyun.hitsdb.client.value.response.LastDPValue;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.response.TagResult;
import com.aliyun.hitsdb.client.value.type.Suggest;

public interface HiTSDB extends Closeable {
	/**
	 * Asynchronous put point
	 * @param point
	 */
	void put(Point point);

	/**
	 * Asynchronous put points
	 * @param points
	 */
	void put(Point... points);

	/**
	 * Synchronous put method
	 * @param points
	 * @return
	 */
	Result putSync(Collection<Point> points);
	
	/**
	 * Synchronous put method
	 * @param points
	 * @return
	 */
	Result putSync(Point... points);

	/**
	 * Synchronous put method
	 * @param points
	 * @return
	 */
	<T extends Result> T putSync(Collection<Point> points, Class<T> resultType);
	
	/**
	 * Synchronous put method
	 * @param points
	 * @return
	 */
	<T extends Result> T putSync(Class<T> resultType,Collection<Point> points);
	
	/**
	 * Synchronous put method
	 * @param points
	 * @return
	 */
	<T extends Result> T putSync(Class<T> resultType,Point... points);
	
	/**
	 * query
	 * @param points
	 * @return
	 */
	void query(Query query, QueryCallback callback);

	/**
	 * query
	 * @param points
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
	 * @param query
	 * @throws HttpUnknowStatusException
	 */
	void delete(Query query) throws HttpUnknowStatusException;

	/**
	 * @param metric
	 * @param startTime
	 * @param endTime
	 * @throws HttpUnknowStatusException
	 */
	void deleteData(String metric, long startTime, long endTime) throws HttpUnknowStatusException;

	/**
	 * @param metric
	 * @param startDate
	 * @param endDate
	 * @throws HttpUnknowStatusException
	 */
	void deleteData(String metric, Date startDate, Date endDate) throws HttpUnknowStatusException;

	/**
	 * @param metric
	 * @param tags
	 * @throws HttpUnknowStatusException
	 */
	void deleteMeta(String metric, Map<String, String> tags) throws HttpUnknowStatusException;

	/**
	 * @param timeline
	 * @throws HttpUnknowStatusException
	 */
	void deleteMeta(Timeline timeline) throws HttpUnknowStatusException;

	/**
	 * @param lifetime
	 * @throws HttpUnknowStatusException
	 */
	void ttl(int lifetime) throws HttpUnknowStatusException;

	/**
	 * @param lifetime
	 * @param unit
	 * @throws HttpUnknowStatusException
	 */
	void ttl(int lifetime, TimeUnit unit) throws HttpUnknowStatusException;

	/**
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	int ttl() throws HttpUnknowStatusException;

	/**
	 * @param type
	 * @param prefix
	 * @param max
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	List<String> suggest(Suggest type, String prefix, int max) throws HttpUnknowStatusException;

	/**
	 * @param tagkey
	 * @param tagValuePrefix
	 * @param max
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException;

	/**
	 * close tsdb
	 * @param force
	 * @throws IOException
	 */
	void close(boolean force) throws IOException;

	/**
	 * @param timelines
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	List<LastDPValue> lastdp(Collection<Timeline> timelines) throws HttpUnknowStatusException;

	/**
	 * @param timelines
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	List<LastDPValue> lastdp(Timeline... timelines) throws HttpUnknowStatusException;
}
