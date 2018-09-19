package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.MultiValuedQueryLastResult;
import com.aliyun.hitsdb.client.value.response.MultiValuedQueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHiTSDBClientMultiValuedFeature {

    private static final Logger LOG = LoggerFactory.getLogger(TestHiTSDBClientMultiValuedFeature.class);

    private static final String MULTI_VALUED_MEASUREMENT_NAME = "measurement_metric";
    private static final String MULTI_VALUED_MEASUREMENT_VALUE1 = "hardware.service";
    private static final String MULTI_VALUED_MEASUREMENT_VALUE2 = "software.service";
    private static final long startTime = 1800000000;

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        BatchPutCallback pcb = new BatchPutCallback(){

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("写入回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("已处理" + count + "个点");
            }

        };

        HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 8242)
                .listenBatchPut(pcb)
                .httpConnectTimeout(90)
                .config();
        tsdb = HiTSDBClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            System.out.println("将要关闭");
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writing data set:
     * Measurement: wind
     * field1: direction
     * field2: speed
     *           Measurement: wind
     * Timestamp   Field: direction  Field: speed
     *  ts1            45.1             1.8
     *  ts2            45.2             1.2
     *  ts3            46               1.9
     *  ts4            47               null
     *  ts5            nul              1.1
     *
     * Query1:
     *      Filter: speed >= 1.1
     *      Expecting Result：
     *         Timestamp   Field: direction  Field: speed
     *         ts1            45.1             1.8
     *         ts2            45.2             1.2
     *         ts3            46               1.9
     *         ts5            null             1.1
     *
     * Query2:
     *      Filter: direction = 46
     *      Expecting Result：
     *         Timestamp   Field: direction  Field: speed
     *         ts3            46               1.9
     *
     * Query3:
     *      Filter: direction >= 45.1 && speed >= 1.1
     *      Expecting Result：
     *          Timestamp   Field: direction  Field: speed
     *             ts1            45.1             1.8
     *             ts2            45.2             1.2
     *             ts3            46               1.9
     */
    @Test
    public void testMultiValuedDataPointPutAndQuery_Basic() {
        long startTimestamp = 1537170208;
        final String MEASUREMENT = "wind";
        final String MEASUREMENT_VALUE = MEASUREMENT + "_dummy";

        final String FIELD1 = "direction";
        final String FIELD2 = "speed";
        final String TAGK1 = "sensor";
        final String TAGV1 = "95D8-7913";
        final String TAGK2 = "city";
        final String TAGV2 = "hangzhou";
        final String TAGK3 = "province";
        final String TAGV3 = "zhejiang";

        final int SIZE = 5;
        // Negative value means null
        double[] field1ValueArrays = { 45.1, 45.2, 46, 47, -1};
        double[] field2ValueArrays = { 1.8, 1.2, 1.9, -1, 1.1};
        long[] timestampArrays = new long[SIZE];
        for(int i = 0; i < SIZE; i++) {
            timestampArrays[i] = startTimestamp + i;
        }

        for(int i = 0; i < SIZE; i++) {
            Double field1Value = (field1ValueArrays[i] >= 0 ? field1ValueArrays[i] : null);
            Double field2Value = (field2ValueArrays[i] >= 0 ? field2ValueArrays[i] : null);
            MultiValuedPoint multiValuedPoint1 = MultiValuedPoint.metric(MEASUREMENT, MEASUREMENT_VALUE)
                    .fields(FIELD1, field1Value)
                    .fields(FIELD2, field2Value)
                    .tag(TAGK1, TAGV1)
                    .tag(TAGK2, TAGV2)
                    .tag(TAGK3, TAGV3)
                    .timestamp(timestampArrays[i])
                    .build();
            tsdb.multiValuedPut(multiValuedPoint1);
        }

        try {
            Thread.sleep(3000);
        } catch (Exception e) {
            // Do nothing
        }
        List<MultiValuedQueryMetricDetails> metricDetails = new ArrayList();

        // Query 1: Filter: speed >= 1.1
        MultiValuedQueryMetricDetails metricDetail_1 = MultiValuedQueryMetricDetails.field(FIELD1).aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_1);
        MultiValuedQueryMetricDetails metricDetail_2 = MultiValuedQueryMetricDetails.field(FIELD2).aggregator(Aggregator.NONE)
                .dpValue(">=1.1")
                .build();
        metricDetails.add(metricDetail_2);
        MultiValuedSubQuery subQuery = MultiValuedSubQuery.metric(MEASUREMENT, MEASUREMENT_VALUE)
                .tag(TAGK1, TAGV1)
                .tag(TAGK2, TAGV2)
                .tag(TAGK3, TAGV3)
                .fieldsInfo(metricDetails)
                .build();
        MultiValuedQuery query = MultiValuedQuery.start(startTimestamp).end(startTimestamp + SIZE).sub(subQuery).build();
        MultiValuedQueryResult result = tsdb.multiValuedQuery(query);
        if (result != null) {
            System.out.println("##### Multi-valued Query Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
        String expectValueString = "[[1537170208, hangzhou, zhejiang, 95D8-7913, wind_dummy, 45.1, 1.8], " +
                "[1537170209, hangzhou, zhejiang, 95D8-7913, wind_dummy, 45.2, 1.2], " +
                "[1537170210, hangzhou, zhejiang, 95D8-7913, wind_dummy, 46.0, 1.9], " +
                "[1537170212, hangzhou, zhejiang, 95D8-7913, wind_dummy, null, 1.1]]";
        Assert.assertEquals(expectValueString, result.getValues().toString());

        // Query 2: Filter: direction = 46
        metricDetails.clear();
        metricDetail_1 = MultiValuedQueryMetricDetails.field(FIELD1).aggregator(Aggregator.NONE)
                .dpValue("=46")
                .build();
        metricDetails.add(metricDetail_1);
        metricDetail_2 = MultiValuedQueryMetricDetails.field(FIELD2).aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_2);
        subQuery = MultiValuedSubQuery.metric(MEASUREMENT, MEASUREMENT_VALUE)
                .tag(TAGK1, TAGV1)
                .tag(TAGK2, TAGV2)
                .tag(TAGK3, TAGV3)
                .fieldsInfo(metricDetails)
                .build();
        query = MultiValuedQuery.start(startTimestamp).end(startTimestamp + SIZE).sub(subQuery).build();
        result = tsdb.multiValuedQuery(query);
        if (result != null) {
            System.out.println("##### Multi-valued Query Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
        expectValueString = "[[1537170210, hangzhou, zhejiang, 95D8-7913, wind_dummy, 46.0, 1.9]]";
        Assert.assertEquals(expectValueString, result.getValues().toString());


        // Query 3: Filter: direction >= 45.1 && speed >= 1.1
        metricDetails.clear();
        metricDetail_1 = MultiValuedQueryMetricDetails.field(FIELD1).aggregator(Aggregator.NONE)
                .dpValue(">=45.1")
                .build();
        metricDetails.add(metricDetail_1);
        metricDetail_2 = MultiValuedQueryMetricDetails.field(FIELD2).aggregator(Aggregator.NONE)
                .dpValue(">=1.1")
                .build();
        metricDetails.add(metricDetail_2);
        subQuery = MultiValuedSubQuery.metric(MEASUREMENT, MEASUREMENT_VALUE)
                .tag(TAGK1, TAGV1)
                .tag(TAGK2, TAGV2)
                .tag(TAGK3, TAGV3)
                .fieldsInfo(metricDetails)
                .build();
        query = MultiValuedQuery.start(startTimestamp).end(startTimestamp + SIZE).sub(subQuery).build();
        result = tsdb.multiValuedQuery(query);
        if (result != null) {
            System.out.println("##### Multi-valued Query Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
        expectValueString = "[[1537170208, hangzhou, zhejiang, 95D8-7913, wind_dummy, 45.1, 1.8], " +
                "[1537170209, hangzhou, zhejiang, 95D8-7913, wind_dummy, 45.2, 1.2], " +
                "[1537170210, hangzhou, zhejiang, 95D8-7913, wind_dummy, 46.0, 1.9]]";
        Assert.assertEquals(expectValueString, result.getValues().toString());
    }


    /**
     * @TCDescription : 多值模型写入
     * @TestStep :
     * @ExpectResult : 成功读取且数据一致
     * @author moqu
     * @modify by chixiao
     * @since 1.0.0
     */
    @Test
    public void testMultiValuedDataPointPut_QUERY_QUERYLAST() {
        Random random = new Random();
        List<Map<String, Object>> softwareExpectingPoints = new ArrayList<Map<String, Object>>();
        List<Map<String, Object>> hardwareExpectingPoints = new ArrayList<Map<String, Object>>();
        List<MultiValuedPoint> softwarePoints = new ArrayList<MultiValuedPoint>();
        List<MultiValuedPoint> hardwarePoints = new ArrayList<MultiValuedPoint>();
        List<MultiValuedPoint> points = new ArrayList<MultiValuedPoint>();
        List<String> expectingColumns = new ArrayList<String>();
        Set<String> tempColumns = new TreeSet<String>();
        long dpsCounter = 100;

        /** Data Point Insertion */
        for (int i = 0; i < dpsCounter; i++) {
            if ( i%2 == 0 ) {
                MultiValuedPoint multiValuedPoint1 = MultiValuedPoint.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE1)
                        .fields("input", i*1.0)
                        .fields("output", i*1.0)
                        .fields("batchId", i*1.0)
                        .fields("osVersion", i*1.0)
                        .tag("iotId", "HDK123456")
                        .tag("productKey", "iPhone123456")
                        .tag("tenantId", "Alibaba")
                        .tag("deviceName", "iPhone_6_Plus")
                        .tag("description", "small_hardware")
                        .timestamp(startTime + i)
                        .build();
                hardwarePoints.add(multiValuedPoint1);
                points.add(multiValuedPoint1);
            }

            if ( i%3 == 0 ) {
                MultiValuedPoint multiValuedPoint2 = MultiValuedPoint.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE1)
                        .fields("input", i*1.0 + 1000)
                        .fields("output", i*1.0 + 1000)
                        .fields("batchId", i*1.0 + 1000)
                        .fields("osVersion", i*1.0 + 1000)
                        .tag("iotId", "HDK654321")
                        .tag("productKey", "iWatch123456")
                        .tag("tenantId", "Tencent")
                        .tag("deviceName", "iWatch_3")
                        .tag("description", "wearable_hardware")
                        .timestamp(startTime + i)
                        .build();
                hardwarePoints.add(multiValuedPoint2);
                points.add(multiValuedPoint2);
            }

            if ( i%5 == 0 ) {
                MultiValuedPoint multiValuedPoint3 = MultiValuedPoint.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE1)
                        .fields("input", i*1.0 + 10000)
                        .fields("output", i*1.0 + 10000)
                        .fields("batchId", i*1.0 + 10000)
                        .fields("value", i*1.0 + 10000)
                        .fields("osVersion", i*1.0 + 10000)
                        .tag("iotId", "HDK654321")
                        .tag("productKey", "iPad123456")
                        .tag("tenantId", "Huaiwei")
                        .tag("deviceName", "iPad_3")
                        .tag("description", "large_hardware")
                        .tag("generation", "2018-06")
                        .timestamp(startTime + i)
                        .build();
                hardwarePoints.add(multiValuedPoint3);
                points.add(multiValuedPoint3);
            }

            MultiValuedPoint multiValuedPoint4 = MultiValuedPoint.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE1)
                    .fields("input", i * 1.0 + 100000)
                    .fields("output", i * 1.0 + 100000)
                    .fields("batchId", i * 1.0 + 100000)
                    .fields("value", i * 1.0 + 100000)
                    .fields("osVersion", i * 1.0 + 100000)
                    .tag("iotId", "HDK123456")
                    .tag("productKey", "MacBook123456")
                    .tag("tenantId", "JD")
                    .tag("deviceName", "MacBook_2015")
                    .tag("description", "laptop")
                    .tag("generation", "2015-08")
                    .timestamp(startTime + i)
                    .build();
            hardwarePoints.add(multiValuedPoint4);
            points.add(multiValuedPoint4);

            constructExpectingTuples(points, hardwareExpectingPoints, startTime+i);
            points.clear();
        }
        tsdb.multiValuedPutSync(hardwarePoints);

        for (int i = 0; i < dpsCounter; i ++) {
            MultiValuedPoint multiValuedPoint5 = MultiValuedPoint.metric(MULTI_VALUED_MEASUREMENT_NAME,MULTI_VALUED_MEASUREMENT_VALUE2)
                    .fields("input", i*1.0 + 1000)
                    .fields("output", i*1.0 + 1000)
                    .fields("batchId", i*1.0 + 1000)
                    .fields("value", i*1.0 + 1000)
                    .fields("release",i*1.0 + 1000)
                    .tag("iotId", "SDK123456")
                    .tag("productKey", "macOS123456")
                    .tag("company", "Apple")
                    .tag("description", "OS")
                    .tag("release", "2018-08")
                    .timestamp(startTime + i)
                    .build();
            points.add(multiValuedPoint5);
            softwarePoints.add(multiValuedPoint5);

            if ( i%2 == 0 ) {
                MultiValuedPoint multiValuedPoint6 = MultiValuedPoint.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE2)
                        .fields("input", i*1.0 + 1000)
                        .fields("output", i*1.0 + 1000)
                        .fields("batchId", i*1.0 + 1000)
                        .fields("value", i*1.0 + 1000)
                        .fields("release", i*1.0 + 1000)
                        .tag("iotId", "SDK123456")
                        .tag("productKey", "iOS123456")
                        .tag("company", "Apple")
                        .tag("description", "OS")
                        .tag("release", "2017-11")
                        .timestamp(startTime + i)
                        .build();
                points.add(multiValuedPoint6);
                softwarePoints.add(multiValuedPoint6);
            }

            constructExpectingTuples(points, softwareExpectingPoints, startTime+i);
            points.clear();
        }
        tsdb.multiValuedPutSync(softwarePoints);

        try {
            Thread.sleep(20000);
        } catch (Exception e) {
            // Do nothing
        }

        /** Multi-valued Query #1 - Measurement: software.service */
        List<MultiValuedQueryMetricDetails> metricDetails = new ArrayList<MultiValuedQueryMetricDetails>();
        // Expectiong Columns : timestamp + tags + fields
        expectingColumns.add("timestamp");
        // 6 Tag Keys
        tempColumns.add("iotId");
        tempColumns.add("productKey");
        tempColumns.add("tenantId");
        tempColumns.add("deviceName");
        tempColumns.add("description");
        tempColumns.add("generation");
        tempColumns.add(MULTI_VALUED_MEASUREMENT_NAME);
        expectingColumns.addAll(tempColumns);
        tempColumns.clear();
        // 5 Field Names
        tempColumns.add("input");
        tempColumns.add("output");
        tempColumns.add("batchId");
        tempColumns.add("value");
        tempColumns.add("osVersion");
        expectingColumns.addAll(tempColumns);
        tempColumns.clear();

        MultiValuedQueryMetricDetails metricDetail_1 = MultiValuedQueryMetricDetails.field("input").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_1);
        MultiValuedQueryMetricDetails metricDetail_2 = MultiValuedQueryMetricDetails.field("output").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_2);
        MultiValuedQueryMetricDetails metricDetail_3 = MultiValuedQueryMetricDetails.field("batchId").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_3);
        MultiValuedQueryMetricDetails metricDetail_4 = MultiValuedQueryMetricDetails.field("value").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_4);
        MultiValuedQueryMetricDetails metricDetail_5 = MultiValuedQueryMetricDetails.field("osVersion").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_5);

        // Query 1 without limit and offset
        MultiValuedSubQuery subQuery = MultiValuedSubQuery.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE1)
                .fieldsInfo(metricDetails)
                .build();
        MultiValuedQuery query = MultiValuedQuery.start(startTime).end(startTime+dpsCounter).sub(subQuery).build();
        MultiValuedQueryResult result = tsdb.multiValuedQuery(query);
        if (result != null) {
            System.out.println("##### Multi-valued Query Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
        verifyDps(expectingColumns, dpsCounter, 0, 0, hardwareExpectingPoints, result);
        System.out.println("###### Multi-valued Query without limit and offset Completed ######");


        // Query 1 with limit and offset
        Integer limit = 20 * (-1);
        Integer offset = 3;
        subQuery = MultiValuedSubQuery.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE1)
                .fieldsInfo(metricDetails)
                .limit(limit).offset(offset)
                .build();
        query = MultiValuedQuery.start(startTime).end(startTime+dpsCounter).sub(subQuery).build();
        result = tsdb.multiValuedQuery(query);
        if (result != null) {
            System.out.println("##### Multi-valued Query Result : " + JSON.toJSONString(result));
            verifyDps(expectingColumns, dpsCounter, limit, offset, hardwareExpectingPoints, result);
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
        System.out.println("###### Multi-valued Query with limit and offset Completed ######");

        /** Multi-valued Query #2 - Measurement: hardware.service */
        metricDetails.clear();
        expectingColumns.clear();
        // Expectiong Columns : timestamp + tags + fields
        expectingColumns.add("timestamp");
        // 6 Tag Keys
        tempColumns.add("iotId");
        tempColumns.add("productKey");
        tempColumns.add("company");
        tempColumns.add("description");
        tempColumns.add("release");
        tempColumns.add(MULTI_VALUED_MEASUREMENT_NAME);
        expectingColumns.addAll(tempColumns);
        tempColumns.clear();
        // 5 Field Names
        tempColumns.add("input");
        tempColumns.add("output");
        tempColumns.add("batchId");
        tempColumns.add("value");
        tempColumns.add("release");
        expectingColumns.addAll(tempColumns);
        tempColumns.clear();

        MultiValuedQueryMetricDetails metricDetail_6 = MultiValuedQueryMetricDetails.field("batchId").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_6);
        MultiValuedQueryMetricDetails metricDetail_7 = MultiValuedQueryMetricDetails.field("value").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_7);
        MultiValuedQueryMetricDetails metricDetail_8 = MultiValuedQueryMetricDetails.field("release").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_8);
        MultiValuedQueryMetricDetails metricDetail_9 = MultiValuedQueryMetricDetails.field("input").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_9);
        MultiValuedQueryMetricDetails metricDetail_10 = MultiValuedQueryMetricDetails.field("output").aggregator(Aggregator.NONE)
                .build();
        metricDetails.add(metricDetail_10);

        subQuery = MultiValuedSubQuery.metric(MULTI_VALUED_MEASUREMENT_NAME, MULTI_VALUED_MEASUREMENT_VALUE2).fieldsInfo(metricDetails).build();
        query = MultiValuedQuery.start(startTime).end(startTime+dpsCounter).sub(subQuery).build();
        result = tsdb.multiValuedQuery(query);
        if (result != null) {
            System.out.println("##### Multi-valued Query Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
        verifyDps(expectingColumns, dpsCounter, 0, 0, softwareExpectingPoints, result);
        System.out.println("###### Multi-valued Query without limit and offset #2 Completed ######");
    }


    public void verifyDps(List<String> expectingColumns, long dpscounter, int limit, int offset, List<Map<String, Object>> expectingDps,
                          MultiValuedQueryResult result) {
        List<Map<String, Object>> resultDps = result.getDps();

        // Checking Expecting Columns
        List<String> resultColumns = new ArrayList<String>();
        resultColumns.addAll(result.getColumns());
        if (!resultColumns.equals(expectingColumns)) {
            LOG.error("Wrong columns. Expecting : {} | Actual : {}", expectingColumns.toString(), resultColumns.toString());
            LOG.error("%%%%%% Query Result : {}", JSON.toJSONString(result));
            Assert.fail("Error! Wrong columns.");
        }

        // Check limit
        if (limit < 0) {
            LOG.info("Reverse Limit Request.");
            limit = limit * (-1);
        }
        if (limit != 0 && limit <= (expectingDps.size() - offset)) {
            if (limit != resultDps.size()) {
                LOG.error("Wrong result size with limit. Expecting : {} | Actual : {}", limit, resultDps.size());
                LOG.error("%%%%%% Query Result : {}", JSON.toJSONString(result));
                Assert.fail("Error! Wrong result size.");
            }
        }

        // Check offset ---- check result set size
        if (offset > 0 && limit == 0) {
            if (expectingDps.size() - resultDps.size() != offset) {
                LOG.error("Wrong offset operation. Expecting Size : {} | Actual Size : {}", expectingDps.size() - offset, resultDps.size());
                Assert.fail("Error! Wrong result size.");
            }
        }

        if (offset == 0 && limit == 0) {
            if (expectingDps.size() != resultDps.size()) {
                LOG.error("Wrong result size without limit. Expecting : {} | Actual : {}", expectingDps.size(), resultDps.size());
                LOG.error("%%%%%% Query Result : {}", JSON.toJSONString(result));
                Assert.fail("Error! Wrong result size.");
            }
        }

        // Check Dps
        for (Map<String, Object> dp : resultDps) {
            Boolean matched = false;
            String expectingDpStr = "";
            for (Map<String, Object> expectingDp : expectingDps) {
                expectingDpStr = JSON.toJSONString(expectingDp);
                if (JSON.toJSONString(dp).equals(expectingDpStr)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                LOG.error("Wrong result content.");
                LOG.error("%%%%%% Expecting : {}", expectingDpStr);
                LOG.error("%%%%%% Actual    : {}", JSON.toJSONString(dp));
                Assert.fail("Error! Wrong result content.");
            }
        }
    }

    public void constructExpectingTuples(List<MultiValuedPoint> points, List<Map<String, Object>> expectingDps, Long timestamp) {
        // All MultiValuedPoint inside points list have the same timestamp
        List<String> columns = new ArrayList<String>();
        columns.add("timestamp");
        // Tags and fields alignment
        Set<String> tagks = new TreeSet<String>();
        Set<String> fields = new TreeSet<String>();
        for (MultiValuedPoint point : points) {
            tagks.addAll(point.getTags().keySet());
            fields.addAll(point.getFields().keySet());
        }
        columns.addAll(tagks);
        columns.addAll(fields);

        // Group points by tags.
        Map<String, List<MultiValuedPoint>> pointsMapWithSameTags = new HashMap<String, List<MultiValuedPoint>>();
        for (MultiValuedPoint point : points) {
            String tags = tagsToString(point.getTags());
//            List<MultiValuedPoint> pointsWithSameTagsList = new ArrayList<>();
//            pointsWithSameTagsList.add(point);
//            List<MultiValuedPoint> existingList = pointsMapWithSameTags.putIfAbsent(tags, pointsWithSameTagsList);
//            if (existingList != null) {
//                existingList.add(point);
//            }

            List<MultiValuedPoint> existingList = pointsMapWithSameTags.get(tags);
            if (existingList == null) {
                existingList = new ArrayList<MultiValuedPoint>();
                pointsMapWithSameTags.put(tags,existingList);
            }
            existingList.add(point);
        }

        // Convert multiple points into tuple format
        for (Map.Entry<String, List<MultiValuedPoint>> entry : pointsMapWithSameTags.entrySet()) {
            Map<String, Object> dataPointTuple = new HashMap();
            dataPointTuple.put("timestamp", timestamp);
            List<MultiValuedPoint> pointsWithSameTags = entry.getValue();
            for (MultiValuedPoint point : pointsWithSameTags) {
                // Tagks
                for (String tagk : point.getTags().keySet()) {
                    dataPointTuple.put(tagk, point.getTags().get(tagk));
                }

                // Fields
                for (String field : point.getFields().keySet()) {
                    dataPointTuple.put(field, point.getFields().get(field));
                }
            }

            // Fill null if necessary
            for (String field : fields) {
//                dataPointTuple.putIfAbsent(field, null);
                if(!dataPointTuple.containsKey(field)){
                    dataPointTuple.put(field,null);
                }

            }

            for (String tagk : tagks) {
//                dataPointTuple.putIfAbsent(tagk, null);
                if(!dataPointTuple.containsKey(tagk)){
                    dataPointTuple.put(tagk,null);
                }

            }
            expectingDps.add(dataPointTuple);
        }
    }

    public String tagsToString(Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        Set<String> tagks = new TreeSet<String>();
        tagks.addAll(tags.keySet());
        StringBuilder tagsString = new StringBuilder();
        boolean firstTag = true;
        for (String tagk : tagks) {
            if (firstTag) {
                tagsString.append(tagk).append("$").append(tags.get(tagk));
                firstTag = false;
            } else {
                tagsString.append("$").append(tagk).append("$").append(tags.get(tagk));
            }
        }
        return tagsString.toString();
    }
}
