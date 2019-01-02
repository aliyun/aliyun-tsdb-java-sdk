package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.*;
import com.aliyun.hitsdb.client.value.type.Suggest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
	 * Asynchronous multi-valued put point
	 * @param point point
	 */
	void multiValuedPut(MultiValuedPoint point);

	/**
	 * Asynchronous multi-valued put points
	 * @param points points
	 */
	void multiValuedPut(MultiValuedPoint... points);

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
	 * @param resultType resultType
	 * @param <T> Result.class, SummaryResult.class, DetailsResult.class
	 * @return Result
	 */
	<T extends Result> T putSync(Collection<Point> points, Class<T> resultType);

	/**
	 * Synchronous put method
	 * @param resultType resultType
	 * @param points points
	 * @param <T> Result.class, SummaryResult.class, DetailsResult.class
	 * @return Result
	 */
	<T extends Result> T putSync(Class<T> resultType,Collection<Point> points);

	/**
	 * Synchronous put method
	 * @param resultType resultType
	 * @param points points
	 * @param <T> Result.class, SummaryResult.class, DetailsResult.class
	 * @return Result
	 */
	<T extends Result> T putSync(Class<T> resultType,Point... points);

	/**
	 * Synchronous multi-valued put method
	 * @param points points
	 * @return Result
	 */
	Result multiValuedPutSync(Collection<MultiValuedPoint> points);

	/**
	 * Synchronous multi-valued put method
	 * @param points points
	 * @return Result
	 */
	Result multiValuedPutSync(MultiValuedPoint... points);

	/**
	 * Synchronous multi-valued put method
	 * @param points points
	 * @param resultType resultType
	 * @param <T> Result.class, SummaryResult.class, DetailsResult.class
	 * @return Result
	 */
	<T extends Result> T multiValuedPutSync(Collection<MultiValuedPoint> points, Class<T> resultType);

	/**
	 * Synchronous multi-valued put method
	 * @param resultType resultType
	 * @param points points
	 * @param <T> Result.class, SummaryResult.class, DetailsResult.class
	 * @return Result
	 */
	<T extends Result> T multiValuedPutSync(Class<T> resultType, Collection<MultiValuedPoint> points);

	/**
	 * Synchronous multi-valued put method
	 * @param resultType resultType
	 * @param points points
	 * @param <T> Result.class, SummaryResult.class, DetailsResult.class
	 * @return Result
	 */
	<T extends Result> T multiValuedPutSync(Class<T> resultType, MultiValuedPoint... points);

	/**
	 * query method
	 * @param query query
	 * @param callback callback
	 */
	void query(Query query, QueryCallback callback);

	/**
	 * query query
	 * @param query query
	 * @return result : List
	 */
	List<QueryResult> query(Query query) throws HttpUnknowStatusException;


	/**
	 * Multi-valued query method. Multi-valued query does not support callback yet.
	 * @param query
	 * @return result : List
	 */
	MultiValuedQueryResult multiValuedQuery(MultiValuedQuery query) throws HttpUnknowStatusException;


	/**
	 * @param query query
	 * @return result : List
	 * @param num point count num
	 * @throws HttpUnknowStatusException Exception
	 */
	List<QueryResult> last(Query query, int num) throws HttpUnknowStatusException;


	/**
	 * delete method
	 * @param query query
	 * @throws HttpUnknowStatusException Exception
	 */
	void delete(Query query) throws HttpUnknowStatusException;

	/**
	 * @param metric metric
	 * @param startTime start timestamp
	 * @param endTime end timestamp
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteData(String metric, long startTime, long endTime) throws HttpUnknowStatusException;

	/**
	 * @param metric metric
	 * @param fields fields that are under above metric
	 * @param startTime start timestamp
	 * @param endTime end timestamp
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteData(String metric, List<String> fields, long startTime, long endTime) throws HttpUnknowStatusException;

	/**
	 * for the api deleteDate
	 * @param metric metric name
	 * @param startDate start date
	 * @param endDate end date
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteData(String metric, Date startDate, Date endDate) throws HttpUnknowStatusException;

	/**
	 * for the api deleteDate
	 * @param metric metric name
	 * @param fields fields that are under above metric
	 * @param startDate start date
	 * @param endDate end date
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteData(String metric, List<String> fields, Date startDate, Date endDate) throws HttpUnknowStatusException;

	/**
	 * delete meta method
	 * @param metric metric
	 * @param tags a map
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteMeta(String metric, Map<String, String> tags) throws HttpUnknowStatusException;

	/**
	 * delete meta method
	 * @param metric metric
	 * @param fields fields that are under above metric
	 * @param tags a map
	 * @throws HttpUnknowStatusException Exception
	 */
	void deleteMeta(String metric, List<String> fields, Map<String, String> tags) throws HttpUnknowStatusException;

	/**
	 * delete meta method
	 * @param timeline timeline object
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
	 * @param type type
	 * @param prefix prefix
	 * @param max max
	 * @return result
	 * @throws HttpUnknowStatusException exception
	 */
	List<String> suggest(Suggest type, String prefix, int max) throws HttpUnknowStatusException;

	/**
	 * suggest method
	 * @param type type
	 * @param metric metric (only applicable to field, tagk, tagv type suggests)
	 * @param prefix prefix
	 * @param max max
	 * @return result
	 * @throws HttpUnknowStatusException exception
	 */
	List<String> suggest(Suggest type, String metric, String prefix, int max) throws HttpUnknowStatusException;

	/**
	 * @param metric
	 * @param tags
	 * @param max
	 * @return
	 */
	List<LookupResult> lookup(String metric, List<LookupTagFilter> tags, int max) throws HttpUnknowStatusException;

	/**
	 * @param lookupRequest
	 * @return
	 */
	List<LookupResult> lookup(LookupRequest lookupRequest) throws HttpUnknowStatusException;

	/**
	 * dumpMeta method
	 * @param tagkey tagkey
	 * @param tagValuePrefix the prefix of the tagvalue
	 * @param max max
	 * @return the List of the TagResult
	 * @throws HttpUnknowStatusException exception
	 */
	List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException;


	/**
	 * dumpMeta method
	 * @param metric metric
	 * @param tagkey tagkey
	 * @param tagValuePrefix the prefix of the tagvalue
	 * @param max max
	 * @return the List of the TagResult
	 * @throws HttpUnknowStatusException exception
	 */
	List<TagResult> dumpMeta(String metric,String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException;


	/**
	 * dumpMetric method
	 * @param tagkey tagkey
	 * @param tagValuePrefix the prefix of the tagvalue
	 * @param max max
	 * @return the List of the TagResult
	 * @throws HttpUnknowStatusException exception
	 */
	List<String> dumpMetric(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException;

	/**
	 * close tsdb method
	 * @param force true or false
	 * @throws IOException exception
	 */
	void close(boolean force) throws IOException;


	/**
	 * /api/query/last endpoint
	 * @param queryLastRequest multi-valued query last request
	 * @return result
	 * @throws HttpUnknowStatusException Exception
	 */
	MultiValuedQueryLastResult multiValuedQueryLast(MultiValuedQueryLastRequest queryLastRequest) throws HttpUnknowStatusException;

	/**
	 * /api/query/last endpoint or /api/query/mlast endpoint
     * depends on whether timelines contain fields information.
	 * @param timelines timelimes
	 * @return result
	 * @throws HttpUnknowStatusException Exception
	 * @deprecated since 0.1.0
	 */
	List<LastDataValue> queryLast(Collection<Timeline> timelines) throws HttpUnknowStatusException;

	/**
	 * /api/query/last endpoint or /api/query/mlast endpoint
     * depends on whether timelines contain fields information.
     * @param timelines timelimes
	 * @return List
	 * @throws HttpUnknowStatusException Exception
	 * @deprecated since 0.1.0
	 */
	List<LastDataValue> queryLast(Timeline... timelines) throws HttpUnknowStatusException;

	/**
	 * /api/query/last endpoint or /api/query/mlast endpoint
     * depends on whether LastPointQuery contains any fields information.
     * @param query
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	List<LastDataValue> queryLast(LastPointQuery query) throws HttpUnknowStatusException;

	/**
	 * /api/query/last endpoint only with tsuids
	 * @param tsuids tsuids
	 * @return result
	 * @throws HttpUnknowStatusException Exception
	 * @deprecated since 0.1.0
	 */
	List<LastDataValue> queryLast(List<String> tsuids) throws HttpUnknowStatusException;

	/**
	 * /api/query/last endpoint only with tsuids
     * This function does not trigger mlast endpoint.
     * @param tsuids tsuids
	 * @return List
	 * @throws HttpUnknowStatusException Exception
	 * @deprecated since 0.1.0
	 */
	List<LastDataValue> queryLast(String... tsuids) throws HttpUnknowStatusException;


	/**
	 * /api/version
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	String version() throws HttpUnknowStatusException;


	/**
	 * /api/version
	 * @return
	 * @throws HttpUnknowStatusException
	 */
	Map<String,String> getVersionInfo() throws HttpUnknowStatusException;

	/**
	 *
	 */
	boolean truncate() throws HttpUnknowStatusException;

	/**
	 * Following APIs are for TSDB's multi-field data model structure's puts and queries.
	 * Since TSDB release 2.3.7
	 */
	/**
     * /api/mput endpoint
	 * Synchronous put method
	 * @param points points
	 * @return Result
	 */
    <T extends Result> T multiFieldPutSync(Collection<MultiFieldPoint> points, Class<T> resultType);

	Result multiFieldPutSync(MultiFieldPoint... points);

	Result multiFieldPutSync(Collection<MultiFieldPoint> points);

	/**
	 * /api/mquery endpoint
     * multi-field query method. Multi-field query does not support callback yet.
	 * @param query
	 * @return result : List
	 */
	List<MultiFieldQueryResult> multiFieldQuery(MultiFieldQuery query) throws HttpUnknowStatusException;

	/**
	 * /api/query/mlast endpoint
     * query fields' latest data points
	 * use this function to get latest data points returned in tuple format
	 * @param lastPointQuery
	 * @return List of MultiFieldQueryLastResult
	 * @throws HttpUnknowStatusException
	 */
	List<MultiFieldQueryLastResult> multiFieldQueryLast(LastPointQuery lastPointQuery) throws HttpUnknowStatusException;
}