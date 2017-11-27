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
	void put(Point point);

	void put(Point... points);

	Result putSync(Collection<Point> points);

	<T extends Result> T putSync(Collection<Point> points, Class<T> resultType);

	void query(Query query, QueryCallback callback);

	List<QueryResult> query(Query query) throws HttpUnknowStatusException;

	List<QueryResult> last(Query query, int num) throws HttpUnknowStatusException;

	void delete(Query query) throws HttpUnknowStatusException;

	void deleteData(String metric, long startTime, long endTime) throws HttpUnknowStatusException;

	void deleteData(String metric, Date startDate, Date endDate) throws HttpUnknowStatusException;

	void deleteMeta(String metric, Map<String, String> tags) throws HttpUnknowStatusException;

	void deleteMeta(Timeline timeline) throws HttpUnknowStatusException;

	void ttl(int lifetime) throws HttpUnknowStatusException;

	void ttl(int lifetime, TimeUnit unit) throws HttpUnknowStatusException;

	int ttl() throws HttpUnknowStatusException;

	List<String> suggest(Suggest type, String prefix, int max) throws HttpUnknowStatusException;

	List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException;

	void close(boolean force) throws IOException;

	List<LastDPValue> lastdp(Collection<Timeline> timelines) throws HttpUnknowStatusException;

	List<LastDPValue> lastdp(Timeline... timelines) throws HttpUnknowStatusException;
}
