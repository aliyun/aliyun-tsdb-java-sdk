package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.callback.*;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.consumer.Consumer;
import com.aliyun.hitsdb.client.consumer.ConsumerFactory;
import com.aliyun.hitsdb.client.exception.http.*;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.HttpClientFactory;
import com.aliyun.hitsdb.client.http.response.HttpStatus;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.queue.DataQueueFactory;
import com.aliyun.hitsdb.client.util.LinkedHashMapUtils;
import com.aliyun.hitsdb.client.util.Objects;
import com.aliyun.hitsdb.client.value.JSONValue;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.*;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.IgnoreErrorsResult;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldIgnoreErrorsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import com.aliyun.hitsdb.client.value.type.Suggest;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;
import com.aliyun.hitsdb.client.value.type.UserPrivilege;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class TSDBClient implements TSDB {
    private static final Logger LOGGER = LoggerFactory.getLogger(TSDBClient.class);
    private final DataQueue queue;
    private final Consumer consumer;
    private final HttpResponseCallbackFactory httpResponseCallbackFactory;
    private final boolean httpCompress;
    private final HttpClient httpclient;
    private RateLimiter rateLimiter;
    private final Config config;
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
        JSON.DEFAULT_PARSER_FEATURE &= ~Feature.UseBigDecimal.getMask();  // disable deserialize a double to BigDecimal
    }

    public TSDBClient(Config config) throws HttpClientInitException {
        this.config = config;
        this.httpclient = HttpClientFactory.createHttpClient(config);
        this.httpCompress = config.isHttpCompress();
        boolean asyncPut = config.isAsyncPut();
        int maxTPS = config.getMaxTPS();
        if (maxTPS > 0) {
            this.rateLimiter = RateLimiter.create(maxTPS);
        }

        if (asyncPut) {
            this.httpResponseCallbackFactory = httpclient.getHttpResponseCallbackFactory();
            int batchPutBufferSize = config.getBatchPutBufferSize();
            int multiFieldBatchPutBufferSize = config.getMultiFieldBatchPutBufferSize();
            int batchPutTimeLimit = config.getBatchPutTimeLimit();
            boolean backpressure = config.isBackpressure();
            this.queue = DataQueueFactory.createDataPointQueue(batchPutBufferSize, multiFieldBatchPutBufferSize, batchPutTimeLimit, backpressure);
            this.consumer = ConsumerFactory.createConsumer(queue, httpclient, rateLimiter, config);
            this.consumer.start();
        } else {
            this.httpResponseCallbackFactory = null;
            this.queue = null;
            this.consumer = null;
        }

        this.httpclient.start();
        LOGGER.info("The tsdb client has started.");

        try {
            this.checkConnection();
        } catch (Exception e) {
            try {
                if (asyncPut) {
                    this.consumer.stop(true);
                }

                this.httpclient.close(true);
                LOGGER.info("when connected to tsdb server failure, so the tsdb client has closed");
            } catch (IOException ex) {
            }
            throw new RuntimeException(e);
        }
    }

    private static final String EMPTY_HOLDER = new JSONObject().toJSONString();
    private static final String VIP_API = "/api/vip_health";

    private void checkConnection() {
        httpclient.post(VIP_API, EMPTY_HOLDER);
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
        LOGGER.info("The tsdb client has closed.");
    }

    @Override
    public void deleteData(String metric, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    @Override
    public void deleteData(String metric, Map<String, String> tags, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, tags, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    @Override
    public void deleteData(String metric, List<String> fields, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, fields, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    @Override
    public void deleteData(String metric, Map<String, String> tags, List<String> fields, long startTime, long endTime) {
        MetricTimeRange metricTimeRange = new MetricTimeRange(metric, tags, fields, startTime, endTime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, metricTimeRange.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    private void handleVoid(ResultResponse resultResponse) {
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
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
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
    public void deleteData(String metric, Map<String, String> tags, Date startDate, Date endDate) {
        long startTime = startDate.getTime();
        long endTime = endDate.getTime();
        deleteData(metric, tags, startTime, endTime);
    }

    @Override
    public void deleteData(String metric, List<String> fields, Date startDate, Date endDate) {
        long startTime = startDate.getTime();
        long endTime = endDate.getTime();
        deleteData(metric, fields, startTime, endTime);
    }

    @Override
    public void deleteData(String metric, Map<String, String> tags, List<String> fields, Date startDate, Date endDate) {
        long startTime = startDate.getTime();
        long endTime = endDate.getTime();
        deleteData(metric, tags, fields, startTime, endTime);
    }

    @Override
    public void deleteMeta(String metric, Map<String, String> tags) {
        Timeline timeline = Timeline.metric(metric).tag(tags).build();
        deleteMeta(timeline);
    }

    @Override
    public void deleteMeta(String metric, List<String> fields, Map<String, String> tags) {
        Timeline timeline = Timeline.metric(metric).tag(tags).fields(fields).build();
        deleteMeta(timeline);
    }

    @Override
    public void deleteMeta(Timeline timeline) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_META, timeline.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    /**
     * The following api are added in the 0.2.5 of the SDK.
     *
     * Ideally, we should implement the void deleteMeta(Timeline timeline, boolean deleteData, boolean recursive)
     * as the prototype implementation and use it for the rest of the 5 api
     *
     * However, if the newly added 3 api issued against the earlier version of the TSDB server,
     * the requests would not be recognised and a error would be returned.
     * For the backward-compatibility, the implementations of the above 3 api would not change.
     */
    @Override
    public void deleteMeta(String metric, Map<String, String> tags, boolean deleteData, boolean recursive) {
        DeleteMetaRequest request = DeleteMetaRequest.metric(metric).tag(tags)
                .deleteData(deleteData).recursive(recursive).build();
        deleteMeta(request);
    }

    @Override
    public void deleteMeta(String metric, List<String> fields, Map<String, String> tags, boolean deleteData, boolean recursive) {
        DeleteMetaRequest request =
                DeleteMetaRequest.metric(metric).tag(tags).fields(fields)
                        .deleteData(deleteData).recursive(recursive).build();
        deleteMeta(request);
    }

    @Override
    public void deleteMeta(DeleteMetaRequest request) {
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_META, request.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    @Override
    public List<TagResult> dumpMeta(String tagkey, String tagValuePrefix, int max) {
        DumpMetaValue dumpMetaValue = new DumpMetaValue(tagkey, tagValuePrefix, max);
        return doDumpMeta(dumpMetaValue);
    }

    /**
     * @param metric         metric
     * @param tagkey         tagkey
     * @param tagValuePrefix the prefix of the tagvalue
     * @param max            max
     * @return
     * @deprecated
     */
    @Override
    public List<TagResult> dumpMeta(String metric, String tagkey, String tagValuePrefix, int max) {
        DumpMetaValue dumpMetaValue = new DumpMetaValue(metric, tagkey, tagValuePrefix, max);
        return doDumpMeta(dumpMetaValue);
    }


    private List<TagResult> doDumpMeta(DumpMetaValue dumpMetaValue) {
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
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public List<String> dumpMetric(String tagkey, String tagValuePrefix, int max) throws HttpUnknowStatusException {
        DumpMetaValue dumpMetaValue = new DumpMetaValue(tagkey, tagValuePrefix, max, true);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DUMP_META, dumpMetaValue.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccess:
                String content = resultResponse.getContent();
                return JSON.parseArray(content, String.class);
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public void put(Point point) {
        queue.send(point);
    }

    @Deprecated
    @Override
    public void multiValuedPut(MultiValuedPoint point) {
        for (Entry<String, Object> field : point.getFields().entrySet()) {
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

    @Deprecated
    @Override
    public MultiValuedQueryResult multiValuedQuery(MultiValuedQuery multiValuedQuery) {
        if (multiValuedQuery.getQueries().size() != 1) {
            LOGGER.error("Sorry. SDK does not support multiple multi-valued sub queries for now.");
            throw new HttpClientException("Sorry. SDK does not support multiple multi-valued sub queries for now.");
        }

        List<SubQuery> singleValuedSubQueries = new ArrayList<SubQuery>();
        Map<String, String> fieldAndDpFilter = new HashMap<String, String>();
        long startTime = multiValuedQuery.getStart();
        long endTime = multiValuedQuery.getEnd();
        for (MultiValuedSubQuery subQuery : multiValuedQuery.getQueries()) {
            for (MultiValuedQueryMetricDetails metricDetails : subQuery.getFieldsInfo()) {
                if (metricDetails.getDpValue() != null && !metricDetails.getDpValue().isEmpty()) {
                    fieldAndDpFilter.put(metricDetails.getField(), metricDetails.getDpValue());
                }
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
                    LOGGER.error("Empty result from TSDB server. {} ", queryResultList.toString());
                    return null;
                }
                setTypeIfNeeded(query, queryResultList);

                // We need to obtain the measurement name from the sub query's metric.
                MultiValuedQueryResult tupleFormat = convertQueryResultIntoTupleFormat(
                        fieldAndDpFilter,
                        queryResultList,
                        multiValuedQuery.getQueries().get(0).getMetric(),
                        multiValuedQuery.getQueries().get(0).getLimit(),
                        multiValuedQuery.getQueries().get(0).getOffset());
                return tupleFormat;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    /**
     * Use {@link Query#isShowType()} to determine whether the data type needs to be displayed,
     * and then use {@link Query#getType()}} to determine whether type inference is required.
     */
    public static void setTypeIfNeeded(final Query query, final List<QueryResult> queryResultList) {
        if (query == null || !query.isShowType()) {
            return;
        }
        final Class<?> queryType = query.getType();
        for (QueryResult queryResult : queryResultList) {
            if (queryType != null) {
                queryResult.setType(queryType);
                continue;
            }
            final LinkedHashMap<Long, Object> dps = queryResult.getDps();
            if (dps == null || dps.size() == 0) {
                continue;
            }
            Class<?> typeExist = null;
            for (Entry<Long, Object> entry : dps.entrySet()) {
                final Long ts = entry.getKey();
                final Object value = entry.getValue();
                final Class<?> type = getType4Single(ts, value, dps);
                if (type == BigDecimal.class) {
                    typeExist = BigDecimal.class;
                    break;
                }
                if (typeExist == null) {
                    typeExist = type;
                    continue;
                }
                // If there is a type inconsistency in the query results,
                // there is only one possibility, that is, the case where long and double coexist.
                // Therefore, the type is inferred as BigDecimal to deal with this special situation.
                if (typeExist != type) {
                    typeExist = BigDecimal.class;
                    break;
                }
            }
            queryResult.setType(typeExist);
        }
    }

    /**
     * Use {@link Query#isShowType()} to determine whether the data type needs to be displayed,
     * and then use {@link Query#getType()}} to determine whether type inference is required.
     */
    public static void setTypeIfNeeded4MultiField(final MultiFieldQuery query, final List<MultiFieldQueryResult> queryResultList) {
        if (query == null || !query.isShowType()) {
            return;
        }
        final List<Class<?>> queryType = query.getTypes();
        for (MultiFieldQueryResult queryResult : queryResultList) {
            if (queryType != null) {
                queryResult.setTypes(queryType);
                continue;
            }
            final List<List<Object>> dps = queryResult.getValues();
            if (dps == null || dps.size() == 0) {
                continue;
            }
            final List<Class<?>> typeList = new LinkedList<Class<?>>();
            final int columnSize = queryResult.getColumns().size();
            for (int i = 1; i < columnSize; i++) {
                Class<?> typeExist = null;
                for (List<Object> dp : dps) {
                    final Object value = dp.get(i);
                    final Class<?> type = getType4Multi(i, value, dp);
                    if (type == BigDecimal.class) {
                        typeExist = BigDecimal.class;
                        break;
                    }
                    if (typeExist == null) {
                        typeExist = type;
                        continue;
                    }
                    // If there is a type inconsistency in the query results,
                    // there is only one possibility, that is, the case where long and double coexist.
                    // Therefore, the type is inferred as BigDecimal to deal with this special situation.
                    if (typeExist != type) {
                        typeExist = BigDecimal.class;
                        break;
                    }
                }
                typeList.add(typeExist);
            }
            queryResult.setTypes(typeList);
        }
    }

    /**
     * Get the data type of the value from data point for single-value situation.
     *
     * @param ts    the timestamp of the data point
     * @param value the value of the data point
     * @param dps   data points
     */
    public static Class<?> getType4Single(Long ts, Object value, LinkedHashMap<Long, Object> dps) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return Long.class;
        } else if (value instanceof Float) {
            return Double.class;
        } else if (value instanceof Double) {
            return Double.class;
        } else return getOtherClass(value);
    }

    /**
     * Get the data type of the value from data point for multi-value situation.
     *
     * @param index the index of multi-value list
     * @param value the value of the field
     * @param dps   data points
     */
    public static Class<?> getType4Multi(int index, Object value, List<Object> dps) {
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return Long.class;
        } else if (value instanceof Float) {
            if (((Float) value) % 1 == 0) {
                dps.set(index, ((Float) value).longValue());
                return Long.class;
            }
            return Double.class;
        } else if (value instanceof Double) {
            if (((Double) value) % 1 == 0) {
                dps.set(index, ((Double) value).longValue());
                return Long.class;
            }
            return Double.class;
        } else {
            return getOtherClass(value);
        }
    }

    /**
     * Handle unusual data types.
     */
    private static Class<?> getOtherClass(Object value) {
        if (value instanceof BigDecimal) {
            return BigDecimal.class;
        } else if (value instanceof Boolean) {
            return Boolean.class;
        } else if (value instanceof String) {
            return String.class;
        } else {
            // If there is a data type that has not been considered,
            // output it as the Object type for the time being,
            // instead of throwing an exception to make the client exit.
            LOGGER.warn("There is a data type that has not been considered, detail: " + value);
            return Object.class;
        }
    }

    /**
     * @param fieldAndDpValueFilter The field and the corresponding dpValue filter.
     * @param queryResults
     * @param metric
     * @param limit
     * @param offset
     * @return
     */
    @Deprecated
    public MultiValuedQueryResult convertQueryResultIntoTupleFormat(Map<String, String> fieldAndDpValueFilter, List<QueryResult> queryResults, String metric,
                                                                    Integer limit, Integer offset) {
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
        if (fieldAndDpValueFilter != null && !fieldAndDpValueFilter.isEmpty()) {
            // Note: If the dpValue filter is not null, the alignedTimestamps set will be intersection based on
            // the field which has dpValue filter.
            boolean firstFlag = true;
            for (QueryResult queryResult : queryResults) {
                String field = queryResult.getMetric();
                if (fieldAndDpValueFilter.containsKey(field)) {
                    if (firstFlag) {
                        alignedTimestamps.addAll(queryResult.getDps().keySet());
                        firstFlag = false;
                    } else {
                        alignedTimestamps.retainAll(queryResult.getDps().keySet());
                    }
                }
                tagks.addAll(queryResult.getTags().keySet());
                fields.add(field);
                aggregateTags.add(queryResult.getAggregateTags());
                dpsCounter += queryResult.getDps().size();
            }
        } else {
            for (QueryResult queryResult : queryResults) {
                alignedTimestamps.addAll(queryResult.getDps().keySet());
                tagks.addAll(queryResult.getTags().keySet());
                fields.add(queryResult.getMetric());
                aggregateTags.add(queryResult.getAggregateTags());
                dpsCounter += queryResult.getDps().size();
            }
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

            List<QueryResult> existingList = queryResultsWithSameTags.get(tags);
            if (existingList == null) {
                existingList = new ArrayList<QueryResult>();
                queryResultsWithSameTags.put(tags, existingList);
            }
            existingList.add(queryResult);
        }

        List<List<Object>> values = new ArrayList<List<Object>>();
        for (long timestamp : alignedTimestamps) {
            for (Entry<String, List<QueryResult>> sameTagsResultList : queryResultsWithSameTags.entrySet()) {
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
                    if (!fieldsMap.containsKey(field)) {
                        fieldsMap.put(field, null);
                    }
                }


                // Format field values and exclude tuples whose fields are all null
                Boolean keepTuple = false;
                for (Entry<String, Object> fieldValue : fieldsMap.entrySet()) {
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
            Map<String, Object> dp = new HashMap();
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
                setTypeIfNeeded(query, queryResultList);
                return queryResultList;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

	@Override
	public SQLResult queryBySQL(String sql) throws HttpUnknowStatusException {
		SQLValue sqlValue = new SQLValue(sql);
		HttpResponse httpResponse = httpclient.post(HttpAPI.SQL, sqlValue.toJSON());
		ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
		HttpStatus httpStatus = resultResponse.getHttpStatus();
		switch (httpStatus) {
			case ServerSuccess:
				String content = resultResponse.getContent();
				return JSON.parseObject(content, SQLResult.class);
			case ServerNotSupport:
				throw new HttpServerNotSupportException(resultResponse);
			case ServerError:
				throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
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
	    return suggest(suggestValue);
    }

	private List<String> suggest(SuggestValue suggestValue) {
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
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
		    default:
		        throw new HttpUnknowStatusException(resultResponse);
		}
	}

	@Override
    public List<String> suggest(Suggest type, String metric, String prefix, int max) {
        SuggestValue suggestValue = new SuggestValue(type.getName(), metric, prefix, max);
		return suggest(suggestValue);
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
                List<LookupResult> list = JSON.parseArray("[" + content + "]", LookupResult.class);
                return list;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public int ttl() {
        HttpResponse httpResponse = httpclient.get(HttpAPI.TTL, null);
        ResultResponse result = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = result.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return 0;
            case ServerSuccess:
                String content = result.getContent();
                TTLResult ttlResult = JSONValue.parseObject(content, TTLResult.class);
                return ttlResult.getVal();
            case ServerNotSupport:
                throw new HttpServerNotSupportException(result);
            case ServerError:
                throw new HttpServerErrorException(result);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(result);
            default:
                throw new HttpUnknowStatusException(result);
        }
    }

    @Override
    public void ttl(int lifetime) {
        TTLValue ttlValue = new TTLValue(lifetime);
        HttpResponse httpResponse = httpclient.post(HttpAPI.TTL, ttlValue.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    @Override
    public void ttl(int lifetime, TimeUnit unit) {
        int seconds = (int) unit.toSeconds(lifetime);
        TTLValue ttlValue = new TTLValue(seconds);
        HttpResponse httpResponse = httpclient.post(HttpAPI.TTL, ttlValue.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
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

    @Deprecated
    @Override
    public Result multiValuedPutSync(MultiValuedPoint... points) {
        return multiValuedPutSync(Arrays.asList(points));
    }

    @Deprecated
    @Override
    public <T extends Result> T multiValuedPutSync(Class<T> resultType, Collection<MultiValuedPoint> points) {
        return multiValuedPutSync(points, resultType);
    }

    @Deprecated
    @Override
    public <T extends Result> T multiValuedPutSync(Class<T> resultType, MultiValuedPoint... points) {
        return multiValuedPutSync(resultType, Arrays.asList(points));
    }

    @Deprecated
    @Override
    public Result multiValuedPutSync(Collection<MultiValuedPoint> points) {
        return multiValuedPutSync(points, Result.class);
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    @Override
    public <T extends Result> T multiValuedPutSync(Collection<MultiValuedPoint> points, Class<T> resultType) {
        List<Point> singleValuedPoints = new ArrayList<Point>();
        for (MultiValuedPoint multiValuedPoint : points) {
            for (Entry<String, Object> field : multiValuedPoint.getFields().entrySet()) {
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
        } else if (resultType.equals(MultiFieldDetailsResult.class)) {
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("details", "true");
            httpResponse = httpclient.post(HttpAPI.PUT, jsonString, paramsMap);
        } else if (resultType.equals(MultiFieldIgnoreErrorsResult.class)) {
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("ignoreErrors", "true");
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
                } else if (resultType.equals(MultiFieldDetailsResult.class)) {
                    result = (T) JSON.parseObject(content, MultiFieldDetailsResult.class);
                } else if (resultType.equals(MultiFieldIgnoreErrorsResult.class)) {
                    result = (T) JSON.parseObject(content, MultiFieldIgnoreErrorsResult.class);
                }

                return result;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public Result putSync(Collection<Point> points) {
        return putSync(points, Result.class);
    }

    /**
     * Synchronous put points with callback
     *
     * @param points
     * @param batchPutCallback
     */
    @Override
    public void put(Collection<Point> points, AbstractBatchPutCallback batchPutCallback) {
        PointsCollection pc = new PointsCollection(points, batchPutCallback);
        this.queue.sendPoints(pc);
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
        } else if (resultType.equals(IgnoreErrorsResult.class)) {
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("ignoreErrors", "true");
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
                } else if (resultType.equals(IgnoreErrorsResult.class)) {
                    result = (T) JSON.parseObject(content, IgnoreErrorsResult.class);
                }

                return result;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
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
        handleVoid(resultResponse);
    }

    @Deprecated
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
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    /**
     * Convert single-valued query results to tuple format.
     * Note: metric is the measurement, which is obtained from MultiValuedQueryLastRequest.
     *
     * @param queryLastResultList single-valued result list
     * @param metric              measurement metric value
     * @return
     */
    @Deprecated
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

            List<LastDataValue> existingList = queryLastResultsWithSameTags.get(tags);
            if (existingList == null) {
                existingList = new ArrayList<LastDataValue>();
                queryLastResultsWithSameTags.put(tags, existingList);
            }
            existingList.add(queryLastResult);
        }

        Set<List<Object>> resultTuples = new TreeSet<List<Object>>(new MultiValuedTupleComparator());

        List<List<Object>> values = new ArrayList<List<Object>>();
        for (Entry<String, List<LastDataValue>> sameTagsResultList : queryLastResultsWithSameTags.entrySet()) {
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
                    if (!fieldsMap.containsKey(field)) {
                        fieldsMap.put(field, null);
                    }
                }

                // Format field values and exclude tuples whose fields are all null
                Boolean keepTuple = false;
                for (Entry<String, Object> fieldValue : fieldsMap.entrySet()) {
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
            Map<String, Object> dp = new HashMap();
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
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
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
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public List<LastDataValue> queryLast(String... tsuids) throws HttpUnknowStatusException {
        return queryLast(Arrays.asList(tsuids));
    }

    private static final String EMPTY_JSON_STR = new JSONObject().toJSONString();

    @Override
    public String version() throws HttpUnknowStatusException {
        HttpResponse httpResponse = httpclient.post(HttpAPI.VERSION, EMPTY_JSON_STR);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return null;
            case ServerSuccess:
                JSONObject result = JSONObject.parseObject(resultResponse.getContent());
                return result.getString("version");
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }


    @Override
    public Map<String, String> getVersionInfo() throws HttpUnknowStatusException {
        HttpResponse httpResponse = httpclient.post(HttpAPI.VERSION, EMPTY_JSON_STR);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return null;
            case ServerSuccess:
                JSONObject result = JSONObject.parseObject(resultResponse.getContent());
                Map<String, String> map = new HashMap<String, String>();
                for (Entry<String, Object> entry : result.entrySet()) {
                    map.put(entry.getKey(), entry.getValue().toString());
                }
                return map;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public void put(Point... points) {
        for (Point p : points) {
            this.put(p);
        }
    }

    @Override
    public void put(Collection<Point> points) {
        for (Point p : points) {
            this.put(p);
        }
    }

    @Deprecated
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

    @Override
    public List<LastDataValue> queryLast(LastPointQuery query) throws HttpUnknowStatusException {
        if (query.getTupleFormat() != null && query.getTupleFormat()) {
            throw new HttpClientException("Tuple format query result is not supported. " +
                    "If you want to query fields' latest data and receive tuple format results, " +
                    "please use multiFieldQueryLast() instead.");
        }
        String jsonString = query.toJSON();
        HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY_LAST, jsonString);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return null;
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<LastDataValue> result = JSON.parseArray(content, LastDataValue.class);
                return result;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public boolean truncate() throws HttpUnknowStatusException {
        HttpResponse httpResponse = httpclient.post(HttpAPI.TRUNCATE, EMPTY_JSON_STR);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
            case ServerSuccess:
                LOGGER.info("truncate result: {}", resultResponse.getContent());
                return true;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public <T extends Result> T multiFieldPutSync(MultiFieldPoint point, Class<T> resultType) {
        return multiFieldPutSync(Collections.singletonList(point), resultType);
    }

    /**
     * Following APIs are for TSDB's multi-field data model structure's puts and queries.
     * Since release TSDB 2.3.7
     */

    /**
     * Synchronous /api/mput endpoint
     *
     * @param points points
     * @return
     */
    @Override
    public <T extends Result> T multiFieldPutSync(Collection<MultiFieldPoint> points, Class<T> resultType) {
        String jsonString = JSON.toJSONString(points, SerializerFeature.DisableCircularReferenceDetect);

        HttpResponse httpResponse;
        if (resultType.equals(Result.class)) {
            httpResponse = httpclient.post(HttpAPI.MPUT, jsonString);
        } else if (resultType.equals(SummaryResult.class)) {
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("summary", "true");
            httpResponse = httpclient.post(HttpAPI.MPUT, jsonString, paramsMap);
        } else if (resultType.equals(MultiFieldDetailsResult.class)) {
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("details", "true");
            httpResponse = httpclient.post(HttpAPI.MPUT, jsonString, paramsMap);
        } else if (resultType.equals(MultiFieldIgnoreErrorsResult.class)) {
            Map<String, String> paramsMap = new HashMap<String, String>();
            paramsMap.put("ignoreErrors", "true");
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
                } else if (resultType.equals(MultiFieldDetailsResult.class)) {
                    result = (T) JSON.parseObject(content, MultiFieldDetailsResult.class);
                } else if (resultType.equals(MultiFieldIgnoreErrorsResult.class)) {
                    result = (T) JSON.parseObject(content, MultiFieldIgnoreErrorsResult.class);
                }

                return result;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public Result multiFieldPutSync(MultiFieldPoint... points) {
        return multiFieldPutSync(Arrays.asList(points));
    }

    @Override
    public Result multiFieldPutSync(Collection<MultiFieldPoint> points) {
        return multiFieldPutSync(points, Result.class);
    }

    /**
     * Synchronous put points with callback
     *
     * @param points
     * @param batchPutCallback
     */
    @Override
    public void multiFieldPut(Collection<MultiFieldPoint> points, AbstractMultiFieldBatchPutCallback batchPutCallback) {
        PointsCollection pc = new PointsCollection(points, batchPutCallback);
        this.queue.sendPoints(pc);
    }

    @Override
    public void multiFieldPut(MultiFieldPoint point) {
        this.queue.sendMultiFieldPoint(point);
    }

    @Override
    public void multiFieldPut(MultiFieldPoint... points) {
        for (MultiFieldPoint p : points) {
            this.queue.sendMultiFieldPoint(p);
        }
    }

    @Override
    public void multiFieldPut(Collection<MultiFieldPoint> points) {
        for (MultiFieldPoint p : points) {
            this.queue.sendMultiFieldPoint(p);
        }
    }

    /**
     * /api/mquery endpoint
     *
     * @param query
     * @return
     * @throws HttpUnknowStatusException
     */
    @Override
    public List<MultiFieldQueryResult> multiFieldQuery(MultiFieldQuery query) throws HttpUnknowStatusException {
        HttpResponse httpResponse = httpclient.post(HttpAPI.MQUERY, query.toJSON());
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return null;
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<MultiFieldQueryResult> queryResultList;
                queryResultList = JSON.parseArray(content, MultiFieldQueryResult.class);
                setTypeIfNeeded4MultiField(query, queryResultList);
                return queryResultList;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    /**
     * /api/query/mlast endpoint
     * LastPointQuery's tupleFormat will be automatically set to true.
     * If customer does not want to receive tuple format query result, please use queryLast() function.
     *
     * @param lastPointQuery
     * @return fields' latest data points in tuple format
     * @throws HttpUnknowStatusException
     */
    @Override
    public List<MultiFieldQueryLastResult> multiFieldQueryLast(LastPointQuery lastPointQuery) throws HttpUnknowStatusException {
        // set the tupleFormat to true automatically
        if ((lastPointQuery.getTupleFormat()== null) || (!lastPointQuery.getTupleFormat())) {
            lastPointQuery.setTupleFormat(true);
        }

        String jsonString = lastPointQuery.toJSON();
        HttpResponse httpResponse = httpclient.post(HttpAPI.QUERY_MLAST, jsonString);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return null;
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<MultiFieldQueryLastResult> result = JSON.parseArray(content, MultiFieldQueryLastResult.class);
                return result;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    /**
     * method POST for the /api/users endpoint to create a new user,
     * which is enabled since TSDB's engine v2.5.13
     *
     * @param username  the name of the user to create
     * @param password  the plain password for the user to create
     * @param privilege the privilege for the user to create
     * @since 0.2.7
     */
    @Override
    public void createUser(String username, String password, UserPrivilege privilege) {
        if ((username == null) || (username.isEmpty()) ||
                (password == null) || (password.isEmpty())) {
            throw new IllegalArgumentException("username or password cannot be empty");
        }

        // UserResult is also the data structure of createUser
        String base64Password = Base64.encodeBase64String(password.getBytes());

        UserResult createRequest = new UserResult(username, base64Password, privilege.id());
        String jsonRequest = createRequest.toJSON();
        HttpResponse httpResponse = httpclient.post(HttpAPI.USER_AUTH, jsonRequest);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    /**
     * method DELETE for the /api/users endpoint to drop an existing user,
     * which is enabled since TSDB's engine v2.5.13
     *
     * @param username the name of the user to create
     * @note if a non-exist username specified, this method would also end normally
     */
    @Override
    public void dropUser(String username) {
        if ((username == null) || (username.isEmpty())) {
            throw new IllegalArgumentException("username cannot be empty");
        }

        HttpResponse httpResponse = httpclient.delete(HttpAPI.USER_AUTH + "?u=" + username, null);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccessNoContent:
                return;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    /**
     * method GET for the /api/users endpoint to drop an existing user,
     * which is enabled since TSDB's engine v2.5.13
     *
     * @return a list of the existing users
     */
    @Override
    public List<UserResult> listUsers() {
        HttpResponse httpResponse = httpclient.get(HttpAPI.USER_AUTH, null);
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccess:
                String content = resultResponse.getContent();
                List<UserResult> result = JSON.parseArray(content, UserResult.class);
                return result;
            case ServerNotSupport:
                throw new HttpServerNotSupportException(resultResponse);
            case ServerError:
                throw new HttpServerErrorException(resultResponse);
            case ServerUnauthorized:
                throw new HttpServerUnauthorizedException(resultResponse);
            default:
                throw new HttpUnknowStatusException(resultResponse);
        }
    }

    @Override
    public void flush() {
        //TODO: it flushes the single field point only, currently
        final Point[] points = this.queue.getPoints();
        if (points == null || points.length == 0) {
            return;
        }
        final int batchPutSize = this.config.getBatchPutSize();
        final ArrayList<Point> pointList = new ArrayList<Point>(points.length);
        Collections.addAll(pointList, points);
        for (int i = 0; i <= points.length - 1; i += batchPutSize) {
            final int endBound = Math.min(points.length, i + batchPutSize);
            final List<Point> sub = pointList.subList(i, endBound);
            this.putSync(sub);
        }
    }
}
