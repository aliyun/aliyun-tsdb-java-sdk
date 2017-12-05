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
	 * @param point point
	 */
	void put(Point point);

	/**
	 * Asynchronous put points
	 * @param points points
	 */
	void put(Point... points);

	/**
	 * Synchronous put method
	 * @param points points
	 * @return Result
	 */
	Result putSync(Collection<Point> points);
	
	/**
	 * Synchronous put method
	 * @param points points
	 * @return Result
	 */
	Result putSync(Point... points);

	/**
	 * Synchronous put method
	 * @param points points
	 * @return Result
	 */
	<T extends Result> T putSync(Collection<Point> points, Class<T> resultType);
	
	/**
	 * Synchronous put method
	 * @param points points
	 * @return Result
	 */
	<T extends Result> T putSync(Class<T> resultType,Collection<Point> points);
	
	/**
	 * Synchronous put method
	 * @param points points
	 * @return Result
	 */
	<T extends Result> T putSync(Class<T> resultType,Point... points);
	
	/**
	 * query query method
	 * @param points points
	 */
	void query(Query query, QueryCallback callback);

	/**
	 * query query
	 * @param points points
	 * @return result : List
	 */
	List<QueryResult> query(Query query) throws HttpUnknowStatusException;

	/**
	 * @param query query
	 * @param num num
	 * @return result : List
	 * @throws HttpUnknowStatusException
	 */
	List<QueryResult> last(Query query, int num) throws HttpUnknowStatusException;

	/**
	 * delete method
	 * @param query query
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
	 * delete meta method
	 * @param metric metric
	 * @param tags a map
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteMeta(String metric, Map<String, String> tags) throws HttpUnknowStatusException;

	/**
	 * delete meta method
	 * @param timeline timeline obj
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteMeta(Timeline timeline) throws HttpUnknowStatusException;

	/**
	 * ttl method
	 * @param lifetime unit:seconds
	 * @throws HttpUnknowStatusException Exception
	 */
	void ttl(int lifetime) throws HttpUnknowStatusException;

	/**
	 * @param lifetime lifetime
	 * @param unit unit 
	 * @throws HttpUnknowStatusException Exception
	 */
	void ttl(int lifetime, TimeUnit unit) throws HttpUnknowStatusException;

	/**
	 * get ttl method
	 * @return ttl seconds
	 * @throws HttpUnknowStatusException Exception
	 */
	int ttl() throws HttpUnknowStatusException;

	/**
	 * suggest method
	 * @param type
	 * @param prefix
	 * @param max
	 * @return List
	 * @throws HttpUnknowStatusException
	 */
	List<String> suggest(Suggest type, String prefix, int max) throws HttpUnknowStatusException;

	/**
	 * dumpMeta method
	 * @param tagkey
	 * @param tagValuePrefix
	 * @param max
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException;

	/**
	 * close tsdb method
	 * @param force
	 * @throws IOException
	 */
	void close(boolean force) throws IOException;

	/**
	 * lastdp
	 * @param timelines timelimes
	 * @return List
	 * @throws HttpUnknowStatusException Exception
	 */
	List<LastDPValue> lastdp(Collection<Timeline> timelines) throws HttpUnknowStatusException;

	/**
	 * lastdp
	 * @param timelines timelimes
	 * @return List
	 * @throws HttpUnknowStatusException Exception
	 */
	List<LastDPValue> lastdp(Timeline... timelines) throws HttpUnknowStatusException;
}
