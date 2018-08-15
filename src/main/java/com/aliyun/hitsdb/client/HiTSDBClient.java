package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.*;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.consumer.Consumer;
import com.aliyun.hitsdb.client.consumer.ConsumerFactory;
import com.aliyun.hitsdb.client.exception.http.HttpClientException;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.exception.http.HttpServerErrorException;
import com.aliyun.hitsdb.client.exception.http.HttpServerNotSupportException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.HttpClientFactory;
import com.aliyun.hitsdb.client.http.response.HttpStatus;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.queue.DataQueueFactory;
import com.aliyun.hitsdb.client.util.LinkedHashMapUtils;
import com.aliyun.hitsdb.client.value.JSONValue;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import com.aliyun.hitsdb.client.value.type.Suggest;
import com.google.common.util.concurrent.RateLimiter;

public class HiTSDBClient implements HiTSDB {
	private static final Logger LOGGER = LoggerFactory.getLogger(HiTSDBClient.class);
	private final DataQueue queue;
	private final Consumer consumer;
	private final HttpResponseCallbackFactory httpResponseCallbackFactory;
	private final boolean httpCompress;
	private final HttpClient httpclient;
	private RateLimiter rateLimter;
	private final HiTSDBConfig config;
	private static Field queryDeleteField;
	static {
		try {
			queryDeleteField = Query.class.getDeclaredField("delete");
			queryDeleteField.setAccessible(true);
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}

	}

	public HiTSDBClient(HiTSDBConfig config) throws HttpClientInitException {
		this.config = config;
		this.httpclient = HttpClientFactory.createHttpClient(config);
		this.httpCompress = config.isHttpCompress();
		boolean asyncPut = config.isAsyncPut();
		int maxTPS = config.getMaxTPS();
		if (maxTPS > 0) {
			this.rateLimter = RateLimiter.create(maxTPS);
		}

		if (asyncPut) {
			this.httpResponseCallbackFactory = httpclient.getHttpResponseCallbackFactory();
			int batchPutBufferSize = config.getBatchPutBufferSize();
			int batchPutTimeLimit = config.getBatchPutTimeLimit();
			boolean backpressure = config.isBackpressure();
			this.queue = DataQueueFactory.createDataPointQueue(batchPutBufferSize, batchPutTimeLimit, backpressure);
			this.consumer = ConsumerFactory.createConsumer(queue, httpclient, rateLimter, config);
			this.consumer.start();
		} else {
			this.httpResponseCallbackFactory = null;
			this.queue = null;
			this.consumer = null;
		}

		this.httpclient.start();
		LOGGER.info("The hitsdb-client has started.");
	}

	@Override
	public void close() throws IOException {
		// 优雅关闭
		this.close(false);
	}

	/**
	 * 强制关闭
	 * 
	 * @throws Exception
	 */
	private void forceClose() throws IOException {
		boolean async = config.isAsyncPut();
		if (async) {
			// 消费者关闭
			this.consumer.stop(true);
		}

		// 客户端关闭
		this.httpclient.close(true);
	}

	/**
	 * 优雅关闭
	 * 
	 * @throws Exception
	 */
	private void gracefulClose() throws IOException {
		boolean async = config.isAsyncPut();

		if (async) {
			// 停止写入
			this.queue.forbiddenSend();

			// 等待队列消费为空
			this.queue.waitEmpty();

			// 消费者关闭
			this.consumer.stop();
		}

		// 客户端关闭
		this.httpclient.close();
	}

	@Override
	public void close(boolean force) throws IOException {
		if (force) {
			forceClose();
		} else {
			gracefulClose();
		}
		LOGGER.info("The hitsdb-client has closed.");
	}

	@Override
	public void deleteData(String metric, long startTime, long endTime) {
		MetricTimeRange metricTimeRange = new MetricTimeRange(metric, startTime, endTime);
		HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccess:
			return;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public void deleteData(String metric, Date startDate, Date endDate) {
		long startTime = startDate.getTime();
		long endTime = endDate.getTime();
		deleteData(metric, startTime, endTime);
	}

	@Override
	public void deleteMeta(String metric, Map<String, String> tags) {
		Timeline timeline = Timeline.metric(metric).tag(tags).build();
		deleteMeta(timeline);
	}

	@Override
	public void deleteMeta(Timeline timeline) {
		HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_META, timeline.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccess:
			return;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) {
		DumpMetaValue dumpMetaValue = new DumpMetaValue(tagkey, tagValuePrefix, max);
		HttpResponse httpResponse = httpclient.post(HttpAPI.DUMP_META, dumpMetaValue.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccess:
			String content = resultResponse.getContent();
			List<TagResult> tagResults = TagResult.parseList(content);
			return tagResults;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}

	}

	@Override
	public void put(Point point) {
		queue.send(point);
	}

	@Override
	public void multiValuedPut(MultiValuedPoint point) {
		for (Map.Entry<String, Object> field : point.getFields().entrySet()) {
			String metric = field.getKey();
			if (metric == null) {
				throw new IllegalArgumentException("Missing field information for multi-valued data points.");
			}
			Point singleValuedPoint = Point.metric(metric)
					.tag(point.getTags())
					.timestamp(point.getTimestamp())
					.value(field.getValue())
					.build();
			queue.send(singleValuedPoint);
		}
	}

    @Override
    public MultiValuedQueryResult multiValuedQuery(MultiValuedQuery multiValuedQuery) {
	    if (multiValuedQuery.getQueries().size() != 1) {
	        LOGGER.error("Sorry. SDK does not support multiple multi-valued sub queries for now.");
	        throw new HttpClientException("Sorry. SDK does not support multiple multi-valued sub queries for now.");
        }

        List<SubQuery> singleValuedSubQueries = new ArrayList<>();
	    long startTime = multiValuedQuery.getStart();
	    long endTime = multiValuedQuery.getEnd();
	    for (MultiValuedSubQuery subQuery : multiValuedQuery.getQueries()) {
            for (MultiValuedQueryMetricDetails metricDetails : subQuery.getFieldsInfo()) {
                SubQuery singleValuedSubQuery = SubQuery.metric(metricDetails.getField()).aggregator(metricDetails.getAggregatorType())
                        .tag(subQuery.getTags())
                        .downsample(metricDetails.getDownsample())
                        .rate(metricDetails.getRate())
                        .dpValue(metricDetails.getDpValue())
                        .build();
                singleValuedSubQueries.add(singleValuedSubQuery);
            }
        }
        Query query = Query.timeRange(startTime, endTime).sub(singleValuedSubQueries).build();

        HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY, query.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return null;
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<QueryResult> queryResultList;
                queryResultList = JSON.parseArray(content, QueryResult.class);
                if (queryResultList == null || queryResultList.isEmpty()) {
                    LOGGER.error("Empty result from HiTSDB server. {} ", queryResultList.toString());
                    return null;
                }

                // We need to obtain the measurement name from the sub query's metric.
                MultiValuedQueryResult tupleFormat = convertQueryResultIntoTupleFormat(queryResultList,
                                                                                       multiValuedQuery.getQueries().get(0).getMetric(),
                                                                                       multiValuedQuery.getQueries().get(0).getLimit(),
                                                                                       multiValuedQuery.getQueries().get(0).getOffset());
                return tupleFormat;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    public MultiValuedQueryResult convertQueryResultIntoTupleFormat(List<QueryResult> queryResults, String metric, Integer limit, Integer offset) {
        long startTime = System.currentTimeMillis();
        long dpsCounter = 0;
        Boolean reverseOrder = false;
	    Set<Long> alignedTimestamps;
	    // Reverse limit?
        if (limit != null && limit < 0) {
            limit = limit * -1;
            reverseOrder = true;
            alignedTimestamps = new TreeSet<Long>(Collections.reverseOrder());
        } else {
            alignedTimestamps = new TreeSet<Long>();
        }
        Set<String> tagks = new TreeSet<String>();
        Set<String> fields = new TreeSet<String>();
        List<List<String>> aggregateTags = new ArrayList<List<String>>();
        if (metric == null || metric.isEmpty()) {
            LOGGER.error("Failed to obtain measurement metric from tags. This should never happen");
            return null;
        }

        // Timestamps, Tagks and Fields alignment based all query result
        for (QueryResult queryResult : queryResults) {
            alignedTimestamps.addAll(queryResult.getDps().keySet());
            tagks.addAll(queryResult.getTags().keySet());
            fields.add(queryResult.getMetric());
            aggregateTags.add(queryResult.getAggregateTags());
            dpsCounter += queryResult.getDps().size();
        }

        /**
         * Final Result Columns: Timestamps + Ordered Tagk + Fields/Metrics
         * Tuples will have columns' values.
         */
        List<String> finalColumns = new ArrayList<String>();
        finalColumns.add("timestamp");
        finalColumns.addAll(tagks);
        finalColumns.addAll(fields);

        Set<List<Object>> resultTuples = new TreeSet<List<Object>>(new MultiValuedTupleComparator(reverseOrder));

        // Group Query Result by tags.
        Map<String, List<QueryResult>> queryResultsWithSameTags = new HashMap<String, List<QueryResult>>();
        for (QueryResult queryResult : queryResults) {
            String tags = queryResult.tagsToString();
            List<QueryResult> queryResultWithSameTagsList = new ArrayList<>();
            queryResultWithSameTagsList.add(queryResult);
            List<QueryResult> existingList = queryResultsWithSameTags.putIfAbsent(tags, queryResultWithSameTagsList);
            if (existingList != null) {
                existingList.add(queryResult);
            }
        }

        List<List<Object>> values = new ArrayList<List<Object>>();
        for (long timestamp : alignedTimestamps) {
            for (Map.Entry<String, List<QueryResult>> sameTagsResultList : queryResultsWithSameTags.entrySet()) {
                List<Object> tupleValues = new ArrayList<Object>();
                tupleValues.add(timestamp);
                Boolean tagValueFilled = false;
                Map<String, Object> fieldsMap = new TreeMap<String, Object>();
                for (int index = 0; index < sameTagsResultList.getValue().size(); index++) {
                    QueryResult result = sameTagsResultList.getValue().get(index);
                    // Fill Tagk values
                    if (!tagValueFilled) {
                        for (String tagk : tagks) {
                            String tagv = result.getTags().get(tagk);
                            tupleValues.add(tagv);
                        }
                        tagValueFilled = true;
                    }

                    // Fill field values
                    fieldsMap.put(result.getMetric(), result.getDps().get(timestamp));
                }

                // Fill field values with null if necessary
                for (String field : fields) {
                    fieldsMap.putIfAbsent(field, null);
                }

                // Format field values and exclude tuples whose fields are all null
                Boolean keepTuple = false;
                for (Map.Entry<String, Object> fieldValue : fieldsMap.entrySet()) {
                    if (fieldValue.getValue() != null) {
                        keepTuple = true;
                    }
                    tupleValues.add(fieldValue.getValue());
                }
                if (keepTuple) {
                    resultTuples.add(tupleValues);
                }
            }
        }

        /** Dps filtering based on limit and offset */
        int dpstartIndex = 0;
        int dpsEndIndex = resultTuples.size();
        if (offset != null && offset >= dpsEndIndex) {
            return null;
        }

        if (!(offset == null || offset == 0) || !(limit == null || limit == 0)) {
            dpstartIndex = offset == null ? 0 : offset;
            int newDpsEndIndex = limit == null ? dpsEndIndex : dpstartIndex + limit;
            if (newDpsEndIndex < dpsEndIndex) {
                dpsEndIndex = newDpsEndIndex;
            }
        }

        values.addAll(resultTuples);
        values = values.subList(dpstartIndex, dpsEndIndex);

        MultiValuedQueryResult tupleFormat = new MultiValuedQueryResult();
        tupleFormat.setName(metric);
        tupleFormat.setColumns(finalColumns);
        tupleFormat.setValues(values);
        tupleFormat.setAggregateTags(aggregateTags);
        // Set dps for easy data access.
        for (List<Object> tupleInfo : values) {
            Map<String, Object> dp = new HashMap<>();
            for (int index = 0; index < finalColumns.size(); index++) {
                dp.put(finalColumns.get(index), tupleInfo.get(index));
            }
            tupleFormat.getDps().add(dp);
        }
        LOGGER.info("Total convertQueryResultIntoTupleFormat conversion time : {}ms. | Total DPS Processed : {}",
                System.currentTimeMillis() - startTime, dpsCounter);
        return tupleFormat;
    }

	@Override
	public List<QueryResult> query(Query query) {
		HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY, query.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccessNoContent:
			return null;
		case ServerSuccess:
			String content = resultResponse.getContent();
			List<QueryResult> queryResultList;
			queryResultList = JSON.parseArray(content, QueryResult.class);
			return queryResultList;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}


	@Override
	public void query(Query query, QueryCallback callback) {

		FutureCallback<HttpResponse> httpCallback = null;
		String address = httpclient.getHttpAddressManager().getAddress();

		if (callback != null) {
			httpCallback = this.httpResponseCallbackFactory.createQueryCallback(address, callback, query);
		}

		httpclient.postToAddress(address, HttpAPI.QUERY, query.toJSON(), httpCallback);
	}

	@Override
	public List<String> suggest(Suggest type, String prefix, int max) {
		SuggestValue suggestValue = new SuggestValue(type.getName(), prefix, max);
		HttpResponse httpResponse = httpclient.post(HttpAPI.SUGGEST, suggestValue.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccess:
			String content = resultResponse.getContent();
			List<String> list = JSON.parseArray(content, String.class);
			return list;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

    @Override
    public List<LookupResult> lookup(String metric, List<LookupTagFilter> tags, int max) {
	    LookupRequest lookupRequest = new LookupRequest(metric, tags, max);
	    return lookup(lookupRequest);
    }

    @Override
    public List<LookupResult> lookup(LookupRequest lookupRequest) {
	    HttpResponse httpResponse = httpclient.post(HttpAPI.LOOKUP, lookupRequest.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<LookupResult> list = JSON.parseArray("["+content+"]", LookupResult.class);
                return list;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

	@Override
	public int ttl() {
		HttpResponse httpResponse = httpclient.get(HttpAPI.TTL, null);
		ResultResponse result = ResultResponse.simplify(httpResponse, this.httpCompress);
		String content = result.getContent();
		TTLResult ttlResult = JSONValue.parseObject(content, TTLResult.class);
		return ttlResult.getVal();
	}

	@Override
	public void ttl(int lifetime) {
		TTLValue ttlValue = new TTLValue(lifetime);
		HttpResponse httpResponse = httpclient.post(HttpAPI.TTL, ttlValue.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccess:
			return;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public void ttl(int lifetime, TimeUnit unit) {
		int seconds = (int) unit.toSeconds(lifetime);
		TTLValue ttlValue = new TTLValue(seconds);
		HttpResponse httpResponse = httpclient.post(HttpAPI.TTL, ttlValue.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccess:
			return;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@SuppressWarnings("deprecation")
    @Override
	public List<QueryResult> last(Query query, int num) throws HttpUnknowStatusException {
		List<QueryResult> queryResults = this.query(query);
		for (QueryResult queryResult : queryResults) {
			{
				LinkedHashMap<Long, Object> dps = queryResult.getDps();
				if (dps != null) {
					LinkedHashMap<Long, Object> newDps = new LinkedHashMap<Long, Object>(num);
					Entry<Long, Object> lastEntry = LinkedHashMapUtils.getTail(dps);
					if (lastEntry != null) {
						newDps.put(lastEntry.getKey(), lastEntry.getValue());
						for (int count = 1; count < num; count++) {
							Entry<Long, Object> beforeEntry = LinkedHashMapUtils.getBefore(lastEntry);
							if (beforeEntry != null) {
								newDps.put(beforeEntry.getKey(), beforeEntry.getValue());
								lastEntry = beforeEntry;
							} else {
								break;
							}
						}
					}

					queryResult.setDps(newDps);
				}
			}

			{
                LinkedHashMap<Long, String> sdps = queryResult.getSdps();
				if (sdps != null) {
					LinkedHashMap<Long, String> newDps = new LinkedHashMap<Long, String>(num);
					Entry<Long, String> lastEntry = LinkedHashMapUtils.getTail(sdps);
					if (lastEntry != null) {
						newDps.put(lastEntry.getKey(), lastEntry.getValue());
						for (int count = 1; count < num; count++) {
							Entry<Long, String> beforeEntry = LinkedHashMapUtils.getBefore(lastEntry);
							if (beforeEntry != null) {
								newDps.put(beforeEntry.getKey(), beforeEntry.getValue());
								lastEntry = beforeEntry;
							} else {
								break;
							}
						}
					}

					queryResult.setSdps(sdps);
				}
			}
		}

		return queryResults;
	}

	@Override
	public Result multiValuedPutSync(MultiValuedPoint... points) {
		return multiValuedPutSync(Arrays.asList(points));
	}

	@Override
	public <T extends Result> T multiValuedPutSync(Class<T> resultType, Collection<MultiValuedPoint> points) {
		return multiValuedPutSync(points, resultType);
	}

	@Override
	public <T extends Result> T multiValuedPutSync(Class<T> resultType, MultiValuedPoint... points) {
		return multiValuedPutSync(resultType, Arrays.asList(points));
	}

	@Override
	public Result multiValuedPutSync(Collection<MultiValuedPoint> points) {
		return multiValuedPutSync(points, Result.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Result> T multiValuedPutSync(Collection<MultiValuedPoint> points, Class<T> resultType) {
	    List<Point> singleValuedPoints = new ArrayList<Point>();
	    for (MultiValuedPoint multiValuedPoint : points) {
	        for (Map.Entry<String, Object> field : multiValuedPoint.getFields().entrySet()) {
	            Point singleValuedPoint = Point.metric(field.getKey())
                        .tag(multiValuedPoint.getTags())
                        .timestamp(multiValuedPoint.getTimestamp())
                        .value(field.getValue())
                        .build();
                singleValuedPoints.add(singleValuedPoint);
            }
        }

		String jsonString = JSON.toJSONString(singleValuedPoints, SerializerFeature.DisableCircularReferenceDetect);

		HttpResponse httpResponse;
		if (resultType.equals(Result.class)) {
			httpResponse = httpclient.post(HttpAPI.PUT, jsonString);
		} else if (resultType.equals(SummaryResult.class)) {
			Map<String, String> paramsMap = new HashMap<String, String>();
			paramsMap.put("summary", "true");
			httpResponse = httpclient.post(HttpAPI.PUT, jsonString, paramsMap);
		} else if (resultType.equals(DetailsResult.class)) {
			Map<String, String> paramsMap = new HashMap<String, String>();
			paramsMap.put("details", "true");
			httpResponse = httpclient.post(HttpAPI.PUT, jsonString, paramsMap);
		} else {
			throw new HttpClientException("This result type is not supported");
		}

		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();

		T result = null;
		switch (httpStatus) {
			case ServerSuccessNoContent:
				result = (T) new Result();
				return result;
			case ServerSuccess:
				String content = resultResponse.getContent();
				if (resultType.equals(SummaryResult.class)) {
					result = (T) JSON.parseObject(content, SummaryResult.class);
				} else if (resultType.equals(DetailsResult.class)) {
					result = (T) JSON.parseObject(content, DetailsResult.class);
				}

				return result;
			case ServerNotSupport:
				throw new HttpServerNotSupportException(resultResponse);
			case ServerError:
				throw new HttpServerErrorException(resultResponse);
			default:
				throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public Result putSync(Collection<Point> points) {
		return putSync(points, Result.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Result> T putSync(Collection<Point> points, Class<T> resultType) {
		String jsonString = JSON.toJSONString(points, SerializerFeature.DisableCircularReferenceDetect);

		HttpResponse httpResponse;
		if (resultType.equals(Result.class)) {
			httpResponse = httpclient.post(HttpAPI.PUT, jsonString);
		} else if (resultType.equals(SummaryResult.class)) {
			Map<String, String> paramsMap = new HashMap<String, String>();
			paramsMap.put("summary", "true");
			httpResponse = httpclient.post(HttpAPI.PUT, jsonString, paramsMap);
		} else if (resultType.equals(DetailsResult.class)) {
			Map<String, String> paramsMap = new HashMap<String, String>();
			paramsMap.put("details", "true");
			httpResponse = httpclient.post(HttpAPI.PUT, jsonString, paramsMap);
		} else {
			throw new HttpClientException("This result type is not supported");
		}

		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();

		T result = null;
		switch (httpStatus) {
		case ServerSuccessNoContent:
			result = (T) new Result();
			return result;
		case ServerSuccess:
			String content = resultResponse.getContent();
			if (resultType.equals(SummaryResult.class)) {
				result = (T) JSON.parseObject(content, SummaryResult.class);
			} else if (resultType.equals(DetailsResult.class)) {
				result = (T) JSON.parseObject(content, DetailsResult.class);
			}

			return result;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public void delete(Query query) throws HttpUnknowStatusException {
		try {
			queryDeleteField.set(query, true);
		} catch (IllegalArgumentException e) {
			throw new HttpClientException(e);
		} catch (IllegalAccessException e) {
			throw new HttpClientException(e);
		}
		HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY, query.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccessNoContent:
			return;
		case ServerSuccess:
			return;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public List<LastDPValue> lastdp(Collection<Timeline> timelines) throws HttpUnknowStatusException {
		Object timelinesJSON = JSON.toJSON(timelines);
		JSONObject obj = new JSONObject();
		obj.put("queries", timelinesJSON);
		String jsonString = obj.toJSONString();
		HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY_LASTDP, jsonString);
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
		case ServerSuccessNoContent:
			return null;
		case ServerSuccess:
			String content = resultResponse.getContent();
			List<LastDPValue> queryResultList = JSON.parseArray(content, LastDPValue.class);
			return queryResultList;
		case ServerNotSupport:
			throw new HttpServerNotSupportException(resultResponse);
		case ServerError:
			throw new HttpServerErrorException(resultResponse);
		default:
			throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public List<LastDPValue> lastdp(Timeline... timelines) throws HttpUnknowStatusException {
		return lastdp(Arrays.asList(timelines));
	}

    @Override
    public MultiValuedQueryLastResult multiValuedQueryLast(MultiValuedQueryLastRequest queryLastRequest) throws HttpUnknowStatusException {
	    List<Timeline> timelines = new ArrayList<Timeline>();

	    // Convert multi-valued query last request to multiple single-value query last requests.
	    for (String field : queryLastRequest.getFields()) {
	        Timeline tl = Timeline.metric(field).tag(queryLastRequest.getTags()).build();
            timelines.add(tl);
        }

        Object timelinesJSON = JSON.toJSON(timelines);
        JSONObject obj = new JSONObject();
        obj.put("queries", timelinesJSON);
        String jsonString = obj.toJSONString();
        HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY_LAST, jsonString);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return null;
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<LastDataValue> queryResultList = JSON.parseArray(content, LastDataValue.class);
                MultiValuedQueryLastResult result = convertQueryLastResultIntoTupleFormat(queryResultList, queryLastRequest.getMetric());
                return result;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    /**
     * Convert single-valued query results to tuple format.
     * Note: metric is the measurement, which is obtained from MultiValuedQueryLastRequest.
     * @param queryLastResultList single-valued result list
     * @param metric measurement metric value
     * @return
     */
    public MultiValuedQueryLastResult convertQueryLastResultIntoTupleFormat(List<LastDataValue> queryLastResultList, String metric) {
        Set<String> tagks = new TreeSet<String>();
        Set<String> fields = new TreeSet<String>();
        if (metric == null || metric.isEmpty()) {
            LOGGER.error("Failed to obtain measurement metric from tags. This should never happen");
            return null;
        }

        // Timestamps, Tagks and Fields alignment based all query result
        for (LastDataValue lastDataValue : queryLastResultList) {
            tagks.addAll(lastDataValue.getTags().keySet());
            fields.add(lastDataValue.getMetric());
        }

        /**
         * Final Result Columns: Timestamps + Ordered Tagk + Fields/Metrics
         * Tuples will have columns' values.
         */
        List<String> finalColumns = new ArrayList<String>();
        finalColumns.add("timestamp");
        finalColumns.addAll(tagks);
        finalColumns.addAll(fields);

        // Group Query Result by tags.
        Map<String, List<LastDataValue>> queryLastResultsWithSameTags = new HashMap<String, List<LastDataValue>>();
        for (LastDataValue queryLastResult : queryLastResultList) {
            String tags = queryLastResult.tagsToString();
            List<LastDataValue> queryLastResultWithSameTagsList = new ArrayList<>();
            queryLastResultWithSameTagsList.add(queryLastResult);
            List<LastDataValue> existingList = queryLastResultsWithSameTags.putIfAbsent(tags, queryLastResultWithSameTagsList);
            if (existingList != null) {
                existingList.add(queryLastResult);
            }
        }

        Set<List<Object>> resultTuples = new TreeSet<List<Object>>(new MultiValuedTupleComparator());

        List<List<Object>> values = new ArrayList<List<Object>>();
        for (Map.Entry<String, List<LastDataValue>> sameTagsResultList : queryLastResultsWithSameTags.entrySet()) {
            // Alignment timestamps with the same tags result list
            Set<Long> alignedTimestamps = new TreeSet<Long>();
            for (LastDataValue lastDataValue : sameTagsResultList.getValue()) {
                alignedTimestamps.add(lastDataValue.getTimestamp());
            }

            for (long timestamp : alignedTimestamps) {
                List<Object> tupleValues = new ArrayList<Object>();
                tupleValues.add(timestamp);
                Boolean tagValueFilled = false;
                Map<String, Object> fieldsMap = new TreeMap<String, Object>();
                for (LastDataValue lastDataValue : sameTagsResultList.getValue()) {
                    // Fill Tagk values
                    if (!tagValueFilled) {
                        for (String tagk : tagks) {
                            String tagv = lastDataValue.getTags().get(tagk);
                            tupleValues.add(tagv);
                        }
                        tagValueFilled = true;
                    }

                    // Fill field values
                    if (lastDataValue.getTimestamp() == timestamp) {
                        fieldsMap.put(lastDataValue.getMetric(), lastDataValue.getValue());
                    }
                }

                // Fill field values with null if necessary
                for (String field : fields) {
                    fieldsMap.putIfAbsent(field, null);
                }

                // Format field values and exclude tuples whose fields are all null
                Boolean keepTuple = false;
                for (Map.Entry<String, Object> fieldValue : fieldsMap.entrySet()) {
                    if (fieldValue.getValue() != null) {
                        keepTuple = true;
                    }
                    tupleValues.add(fieldValue.getValue());
                }
                if (keepTuple) {
                    resultTuples.add(tupleValues);
                }
            }
        }

        values.addAll(resultTuples);

        MultiValuedQueryLastResult tupleFormat = new MultiValuedQueryLastResult();
        tupleFormat.setName(metric);
        tupleFormat.setColumns(finalColumns);
        tupleFormat.setValues(values);
        // Set dps for easy data access.
        for (List<Object> tupleInfo : values) {
            Map<String, Object> dp = new HashMap<>();
            for (int index = 0; index < finalColumns.size(); index++) {
                dp.put(finalColumns.get(index), tupleInfo.get(index));
            }
            tupleFormat.getDps().add(dp);
        }
        return tupleFormat;

    }

	@Override
	public List<LastDataValue> queryLast(Collection<Timeline> timelines) throws HttpUnknowStatusException {
		Object timelinesJSON = JSON.toJSON(timelines);
		JSONObject obj = new JSONObject();
		obj.put("queries", timelinesJSON);
		String jsonString = obj.toJSONString();
		HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY_LAST, jsonString);
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
			case ServerSuccessNoContent:
				return null;
			case ServerSuccess:
				String content = resultResponse.getContent();
				List<LastDataValue> queryResultList = JSON.parseArray(content, LastDataValue.class);
				return queryResultList;
			case ServerNotSupport:
				throw new HttpServerNotSupportException(resultResponse);
			case ServerError:
				throw new HttpServerErrorException(resultResponse);
			default:
				throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public List<LastDataValue> queryLast(Timeline... timelines) throws HttpUnknowStatusException {
		return queryLast(Arrays.asList(timelines));
	}

	@Override
	public List<LastDataValue> queryLast(List<String> tsuids) throws HttpUnknowStatusException {
		Object tsuidsJSONList = JSON.toJSON(tsuids); /* Convert to ["000001000001000001","000001000001000002,...] */
		JSONObject tsuidsJSONObj = new JSONObject();
		tsuidsJSONObj.put("tsuids", tsuidsJSONList); /* Convert to "tsuid":["000001000001000001","000001000001000002,...] */
        List<JSONObject> tsuidsJSONObjList = new LinkedList<JSONObject>();
        tsuidsJSONObjList.add(tsuidsJSONObj);
		JSONObject obj = new JSONObject();
		obj.put("queries", tsuidsJSONObjList); /* Convert to "queries":[{"tsuid":["000001000001000001","000001000001000002,...]}] */
		String jsonString = obj.toJSONString();
		HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY_LAST, jsonString);
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
			case ServerSuccessNoContent:
				return null;
			case ServerSuccess:
				String content = resultResponse.getContent();
				List<LastDataValue> queryResultList = JSON.parseArray(content, LastDataValue.class);
				return queryResultList;
			case ServerNotSupport:
				throw new HttpServerNotSupportException(resultResponse);
			case ServerError:
				throw new HttpServerErrorException(resultResponse);
			default:
				throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
	public List<LastDataValue> queryLast(String... tsuids) throws HttpUnknowStatusException {
		return queryLast(Arrays.asList(tsuids));
	}

	@Override
	public void put(Point... points) {
		for (Point p : points) {
			this.put(p);
		}
	}

	@Override
	public void multiValuedPut(MultiValuedPoint... points) {
		for (MultiValuedPoint p : points) {
			this.multiValuedPut(p);
		}
	}

	@Override
	public Result putSync(Point... points) {
		return putSync(Arrays.asList(points));
	}

	@Override
	public <T extends Result> T putSync(Class<T> resultType, Collection<Point> points) {
		return putSync(points, resultType);
	}

	@Override
	public <T extends Result> T putSync(Class<T> resultType, Point... points) {
		return putSync(resultType, Arrays.asList(points));
	}

}
