package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.MultiFieldQuery;
import com.aliyun.hitsdb.client.value.request.MultiFieldSubQuery;
import com.aliyun.hitsdb.client.value.request.MultiFieldSubQueryDetails;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.After;
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
    public void testMultiFieldDataPointPutAndQuery_Basic() {
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
                    .fields(FIELD1, field1Value)
                    .fields(FIELD2, field2Value)
                    .fields(FIELD3, field3Value)
                    .fields(FIELD4, field4Value)
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
    public void testMultiFieldDataPointPut_QUERY_QUERYLAST() {
        //
    }
}
