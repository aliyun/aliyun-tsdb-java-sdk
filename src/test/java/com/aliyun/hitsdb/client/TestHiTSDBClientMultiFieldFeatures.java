package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.LastDataValue;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryLastResult;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.DownsampleDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TestHiTSDBClientMultiFieldFeatures {
    private static final Logger LOG = LoggerFactory.getLogger(TestHiTSDBClientMultiFieldFeatures.class);

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 8242)
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
     * Query1:
     *      Filter: speed >= 45.2 && level >= 1.2
     *      Expecting Result：
     *         Timestamp   Field: direction  Field: speed
     *         ts1            45.1             1.8
     *         ts2            45.2             1.2
     *         ts3            46               1.9
     *         ts5            null             1.1
     *
     * Query2:
     *      Filter: speed = 46 && 2s downsample
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
    public void testMultiFieldPutAndQuery() {
        long startTimestamp = 1537170208;
        final String metric = "wind";

        final String FIELD1 = "speed";
        final String FIELD2 = "level";
        final String FIELD3 = "direction";
        final String FIELD4 = "description";
        final String TAGK1 = "sensor";
        final String TAGV1 = "95D8-7913";
        final String TAGK2 = "city";
        final String TAGV2 = "hangzhou";
        final String TAGK3 = "province";
        final String TAGV3 = "zhejiang";

        final int SIZE = 5;
        // Negative value means null
        double[] field1ValueArrays = {45.1, 45.2, 46, 47, 43.8};
        double[] field2ValueArrays = {1.8, 1.2, 1.9, 0.9, 1.1};
        String[] field3ValueArrays = {"E", "S", "W", "N", "NE"};
        String[] field4ValueArrays = {"Fresh Breeze", "Breeze", "Fresh Breeze", "Breeze", "Fresh Breeze"};
        long[] timestampArrays = new long[SIZE];
        for(int i = 0; i < SIZE; i++) {
            timestampArrays[i] = startTimestamp + i;
        }

        for(int i = 0; i < SIZE; i++) {
            Double field1Value = field1ValueArrays[i];
            Double field2Value = field2ValueArrays[i];
            String field3Value = field3ValueArrays[i];
            String field4Value = field4ValueArrays[i];

            MultiFieldPoint multiFieldPoint = MultiFieldPoint.metric(metric)
                    .field(FIELD1, field1Value)
                    .field(FIELD2, field2Value)
                    .field(FIELD3, field3Value)
                    .field(FIELD4, field4Value)
                    .tag(TAGK1, TAGV1)
                    .tag(TAGK2, TAGV2)
                    .tag(TAGK3, TAGV3)
                    .timestamp(timestampArrays[i])
                    .build();
            tsdb.multiFieldPutSync(multiFieldPoint);
        }

        List<MultiFieldSubQueryDetails> fieldsDetails = new ArrayList();

        // Query 1: Filter: level >= 1.2 & speed >= 45.2
        MultiFieldSubQueryDetails fieldDetail_1 = MultiFieldSubQueryDetails.field(FIELD1).aggregator(Aggregator.NONE)
                .alias("speed_output")
                .dpValue(">=45.2")
                .build();
        fieldsDetails.add(fieldDetail_1);
        MultiFieldSubQueryDetails fieldDetail_2 = MultiFieldSubQueryDetails.field(FIELD2).aggregator(Aggregator.NONE)
                .dpValue(">=1.2")
                .build();
        fieldsDetails.add(fieldDetail_2);
        MultiFieldSubQueryDetails fieldDetail_3 = MultiFieldSubQueryDetails.field(FIELD3).aggregator(Aggregator.NONE)
                .build();
        fieldsDetails.add(fieldDetail_3);
        MultiFieldSubQueryDetails fieldDetail_4 = MultiFieldSubQueryDetails.field(FIELD4).aggregator(Aggregator.NONE)
                .build();
        fieldsDetails.add(fieldDetail_4);
        MultiFieldSubQuery subQuery = MultiFieldSubQuery.metric(metric)
                .tag(TAGK1, TAGV1)
                .tag(TAGK2, TAGV2)
                .tag(TAGK3, TAGV3)
                .fieldsInfo(fieldsDetails)
                .build();
        MultiFieldQuery query = MultiFieldQuery.start(startTimestamp).end(startTimestamp + SIZE).sub(subQuery).build();
        List<MultiFieldQueryResult> result = tsdb.multiFieldQuery(query);
        if (result != null) {
            System.out.println("##### Multi-field Query Result : " + JSON.toJSONString(result));
            if (result.size() > 0) {
                System.out.println("##### Multi-field Query Result asMap : " + JSON.toJSONString(result.get(0).asMap()));
            }
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }

        // Query 2: Filter: speed = 46.0
        fieldsDetails.clear();
        fieldDetail_1 = MultiFieldSubQueryDetails.field(FIELD1).aggregator(Aggregator.SUM)
                .downsample("2s-avg")
                .dpValue("=46.5")
                .build();
        fieldsDetails.add(fieldDetail_1);
        fieldDetail_2 = MultiFieldSubQueryDetails.field(FIELD2).aggregator(Aggregator.COUNT)
                .downsample("2s-sum")
                .build();
        fieldsDetails.add(fieldDetail_2);
        fieldDetail_3 = MultiFieldSubQueryDetails.field(FIELD3).aggregator(Aggregator.COUNT)
                .downsample("2s-count")
                .build();
        fieldsDetails.add(fieldDetail_3);
        fieldDetail_4 = MultiFieldSubQueryDetails.field(FIELD4).aggregator(Aggregator.COUNT)
                .downsample("2s-last")
                .build();
        fieldsDetails.add(fieldDetail_4);

        subQuery = MultiFieldSubQuery.metric(metric)
                .tag(TAGK1, TAGV1)
                .tag(TAGK2, TAGV2)
                .tag(TAGK3, TAGV3)
                .fieldsInfo(fieldsDetails)
                .build();
        query = MultiFieldQuery.start(startTimestamp).end(startTimestamp + SIZE).sub(subQuery).build();
        result = tsdb.multiFieldQuery(query);
        if (result != null) {
            System.out.println("##### Multi-field Query Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }

        // Query 3: Filter: direction >= 45.1 && speed >= 1.1
        fieldsDetails.clear();
        fieldDetail_1 = MultiFieldSubQueryDetails.field(FIELD1).aggregator(Aggregator.NONE)
                .dpValue("<=45.9")
                .build();
        fieldsDetails.add(fieldDetail_1);
        fieldDetail_2 = MultiFieldSubQueryDetails.field(FIELD2).aggregator(Aggregator.NONE)
                .dpValue("<=1.6")
                .build();
        fieldsDetails.add(fieldDetail_2);
        fieldDetail_3 = MultiFieldSubQueryDetails.field("*").aggregator(Aggregator.NONE)
                .alias("noagg_output_")
                .build();
        fieldsDetails.add(fieldDetail_3);
        subQuery = MultiFieldSubQuery.metric(metric)
                .tag(TAGK1, TAGV1)
                .tag(TAGK2, TAGV2)
                .tag(TAGK3, TAGV3)
                .fieldsInfo(fieldsDetails)
                .build();
        query = MultiFieldQuery.start(startTimestamp).end(startTimestamp + SIZE).sub(subQuery).build();
        result = tsdb.multiFieldQuery(query);
        if (result != null) {
            System.out.println("##### Multi-field Query Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
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
    public void testMultiFieldQueryLast() {
        String metric = "wind";
        List<String> fields = new ArrayList<String>();
        fields.add("direction");
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("sensor", "95D8-7913");
        LastPointQuery lastPointQuery = LastPointQuery.builder()
                .sub(LastPointSubQuery.builder(metric, fields, tags).build()).tupleFormat(true).build();

        List<MultiFieldQueryLastResult> result = tsdb.multiFieldQueryLast(lastPointQuery);
        if (result != null) {
            System.out.println("##### Multi-field Query Last Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from HiTSDB server. ######");
        }
    }

    /**
     * test the basic behavior of the where interface of SDK
     * @note it is required that the version of TSDB engine greater than v2.6.1
     */
    @Test
    public void testMultiFieldQueryWithWhereClause() {
        final String METRIC = "testMultiFieldQueryWithWhereClause", TAGK = "tagk";

        try {
            tsdb.deleteMeta(Timeline.metric(METRIC).build());
            Thread.sleep(3000);
        } catch (Exception e) {
            System.out.println("Failed to delete time series during pre-testing stage. Continue.");
        }

        List<MultiFieldPoint> mpoints = new ArrayList<MultiFieldPoint>();
        String [] strings = new String[]{"abc", "ABC", "11.11a"};

        long startTime = 1514736000L;
        // timeline1
        for (int i = 0; i < 4; i++) {
            mpoints.add(MultiFieldPoint.metric(METRIC)
                    .tag(TAGK, "tagv1")
                    .field("f1", 100 + i)
                    .field("f2", 333.33)
                    .field("f3", 300 - i * 100)
                    .field("f4", strings[i % 3])
                    .timestamp(startTime + i * 30).build());
        }

        // timeline2, no field3
        mpoints.add(MultiFieldPoint.metric(METRIC)
                .tag(TAGK, "tagv2")
                .field("f1", 100)
                .field("f2", 222.22)
                .field("f4", strings[2 % 3])
                .timestamp(startTime + 90).build());

        // timeline2, no field4
        mpoints.add(MultiFieldPoint.metric(METRIC)
                .tag(TAGK, "tagv3")
                .field("f1", 105)
                .field("f2", 111.11)
                .field("f3", 100)
                .timestamp(startTime + 120).build());

        tsdb.multiFieldPutSync(mpoints);


        MultiFieldQuery mq = MultiFieldQuery.start(startTime)
                .sub(MultiFieldSubQuery
                        .metric(METRIC)
                        .fieldsInfo(MultiFieldSubQueryDetails
                                .aggregator(Aggregator.NONE)
                                .field("*")
                                .where("f3=100")
                                .build())
                        .build())
                .build();
        List<MultiFieldQueryResult> results = tsdb.multiFieldQuery(mq);

        Assert.assertEquals(3, results.size());

        for (MultiFieldQueryResult r: results) {
            Assert.assertEquals(5, r.getColumns().size());

            if (r.getTags().get(TAGK).equals("tagv1")) {
                Assert.assertEquals(1, r.getValues().size());
            } else if (r.getTags().get(TAGK).equals("tagv2")) {
                Assert.assertEquals(0, r.getValues().size());
            } else if (r.getTags().get(TAGK).equals("tagv3")) {
                Assert.assertEquals(1, r.getValues().size());
            } else {
                Assert.fail("unexpected tag value retrieved: " + r.getTags().get(TAGK));
            }
        }
    }

    /**
     * test the basic behavior of the limit and global interface of SDK
     * @note it is required that the version of TSDB engine greater than v2.6.13
     * Q1: MQuery + rlimit + roffset    Q2: MLast + limit + rlimit + roffset
     * Q3: MLast + limit + rlimit    Q4: MLast + limit + roffset
     */
    @Test
    public void testMultiFieldQueryWithRLimitAndRoffset() {
        final String METRIC = "testMultiFieldQueryWithLimit", TAGK = "tagk";

        try {
            tsdb.deleteMeta(Timeline.metric(METRIC).build());
            Thread.sleep(3000);
        } catch (Exception e) {
            System.out.println("Failed to delete time series during pre-testing stage. Continue.");
        }

        List<MultiFieldPoint> mpoints = new ArrayList<MultiFieldPoint>();
        String [] strings = new String[]{"abc", "ABC", "11.11a"};

        long startTime = 1514736000L;
        // timeline1
        for (int i = 0; i < 4; i++) {
            mpoints.add(MultiFieldPoint.metric(METRIC)
                    .tag(TAGK, "tagv1")
                    .field("f1", 100 + i)
                    .field("f2", 333.33)
                    .field("f3", 300 - i * 100)
                    .field("f4", strings[i % 3])
                    .timestamp(startTime + i * 30).build());
        }

        // timeline2, no field3
        for (int i = 0; i < 4; i++) {
            mpoints.add(MultiFieldPoint.metric(METRIC)
                    .tag(TAGK, "tagv2")
                    .field("f1", 100 + i)
                    .field("f2", 333.33)
                    .field("f3", 300 - i * 100)
                    .timestamp(startTime + i * 30).build());
        }

        tsdb.multiFieldPutSync(mpoints);
        // Q1: MQuery + rlimit + roffset
        MultiFieldQuery mq = MultiFieldQuery.start(startTime)
                .sub(MultiFieldSubQuery
                        .metric(METRIC)
                        .limit(3)
                        .rlimit(1)
                        .roffset(1)
                        .fieldsInfo(MultiFieldSubQueryDetails
                                .aggregator(Aggregator.NONE)
                                .field("*")
                                .build())
                        .build())
                .build();
        List<MultiFieldQueryResult> results = tsdb.multiFieldQuery(mq);

        Assert.assertEquals(2, results.size());

        for (MultiFieldQueryResult r: results) {
            Assert.assertEquals(5, r.getColumns().size());

            if (r.getTags().get(TAGK).equals("tagv1")) {
                Assert.assertEquals(1, r.getValues().size());
                Assert.assertEquals("[[1514736030, 101.0, 333.33, 200.0, ABC]]", r.getValues().toString());
            } else if (r.getTags().get(TAGK).equals("tagv2")) {
                Assert.assertEquals(1, r.getValues().size());
                Assert.assertEquals("[[1514736030, 101.0, 333.33, 200.0, null]]", r.getValues().toString());
            } else {
                Assert.fail("unexpected tag value retrieved: " + r.getTags().get(TAGK));
            }
        }

        // Q2: MLast + rlimit + roffset
        LastPointQuery lastPointQuery = LastPointQuery.builder()
                .limit(new LastLimit(startTime, 3))
                .rlimit(1)
                .roffset(1)
                .sub(LastPointSubQuery.builder(METRIC, Collections.singletonList("*"), Collections.<String, String>emptyMap()).build())
                .tupleFormat(true).build();
        List<MultiFieldQueryLastResult> lastResults = tsdb.multiFieldQueryLast(lastPointQuery);
        Assert.assertEquals(2, lastResults.size());
        for (MultiFieldQueryLastResult multiFieldQueryLastResult : lastResults) {
            Assert.assertEquals(1, multiFieldQueryLastResult.getValues().size());
            if (multiFieldQueryLastResult.getTags().get("tagk").equals("tagv1")) {
                Assert.assertEquals("[1514736060000, 102.0, 333.33, 100.0, 11.11a]", multiFieldQueryLastResult.getValues().get(0).toString());
            } else {
                Assert.assertEquals("[1514736060000, 102.0, 333.33, 100.0]", multiFieldQueryLastResult.getValues().get(0).toString());
            }
        }

        // Q3: MLast + limit + rlimit
        lastPointQuery = LastPointQuery.builder()
                .limit(new LastLimit(startTime, 3))
                .rlimit(1)
                .sub(LastPointSubQuery.builder(METRIC, Collections.singletonList("*"), Collections.<String, String>emptyMap()).build())
                .tupleFormat(true).build();
        lastResults = tsdb.multiFieldQueryLast(lastPointQuery);
        Assert.assertEquals(2, lastResults.size());
        for (MultiFieldQueryLastResult multiFieldQueryLastResult : lastResults) {
            Assert.assertEquals(1, multiFieldQueryLastResult.getValues().size());
            if (multiFieldQueryLastResult.getTags().get("tagk").equals("tagv1")) {
                Assert.assertEquals("[1514736030000, 101.0, 333.33, 200.0, ABC]", multiFieldQueryLastResult.getValues().get(0).toString());
            } else {
                Assert.assertEquals("[1514736030000, 101.0, 333.33, 200.0]", multiFieldQueryLastResult.getValues().get(0).toString());
            }
        }

        // Q3: MLast + limit + roffset
        lastPointQuery = LastPointQuery.builder()
                .limit(new LastLimit(startTime, 3))
                .roffset(1)
                .sub(LastPointSubQuery.builder(METRIC, Collections.singletonList("*"), Collections.<String, String>emptyMap()).build())
                .tupleFormat(true).build();
        lastResults = tsdb.multiFieldQueryLast(lastPointQuery);
        Assert.assertEquals(2, lastResults.size());
        for (MultiFieldQueryLastResult multiFieldQueryLastResult : lastResults) {
            Assert.assertEquals(2, multiFieldQueryLastResult.getValues().size());
            if (multiFieldQueryLastResult.getTags().get("tagk").equals("tagv1")) {
                Assert.assertEquals("[1514736060000, 102.0, 333.33, 100.0, 11.11a]", multiFieldQueryLastResult.getValues().get(0).toString());
            } else {
                Assert.assertEquals("[1514736060000, 102.0, 333.33, 100.0]", multiFieldQueryLastResult.getValues().get(0).toString());
            }
        }
    }
}
