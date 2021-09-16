package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.MultiFieldQuery;
import com.aliyun.hitsdb.client.value.request.MultiFieldSubQuery;
import com.aliyun.hitsdb.client.value.request.MultiFieldSubQueryDetails;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.DownsampleDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestLindormTSDBClient {
    LindormTSDBClient tsdb;

    @Before
    public void init() throws HttpClientInitException {
        TSDBConfig config = TSDBConfig
                .address("localhost", 3002)
                .config();
        tsdb = LindormTSDBClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeleteMetric() {
        tsdb.deleteData("test");
    }

    @Test
    public void testDeleteMetricWithTimeRange() {
        try {
            tsdb.deleteData("test", System.currentTimeMillis() - 1, System.currentTimeMillis());
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "delete data with time range is not supported");
        }
    }

    @Test
    public void testMultiFieldPut() {
        long startTime = System.currentTimeMillis();
        double value = Math.random();
        MultiFieldPoint point = MultiFieldPoint.metric("test")
                .tag("_id", "1")
                .field("field1", value)
                .timestamp(startTime)
                .build();
        Result result = tsdb.multiFieldPutSync(point);
        if (result != null) {
            System.out.println("##### Multi-field Put Result : " + JSON.toJSONString(result));
        } else {
            System.out.println("##### Empty reply from TSDB server. ######");
        }
    }

    @Test
    public void testMultiFieldQueryWithDownsampleDataSource() {
        long startTimestamp = System.currentTimeMillis();
        final String metric = "wind";
        final String field = "speed";

        final int SIZE = 5;
        for (int i = 0; i < SIZE; i++) {
            MultiFieldPoint multiFieldPoint = MultiFieldPoint.metric(metric)
                    .field(field, 1)
                    .timestamp(startTimestamp + 1)
                    .build();
            tsdb.multiFieldPutSync(multiFieldPoint);
        }

        MultiFieldSubQueryDetails fieldSubQueryDetails = MultiFieldSubQueryDetails.field(field).aggregator(Aggregator.NONE)
                .downsample("5s-sum")
                .build();
        MultiFieldSubQuery subQuery = MultiFieldSubQuery.metric(metric)
                .fieldsInfo(fieldSubQueryDetails)
                .downsampleDataSource(DownsampleDataSource.DOWNSAMPLE)
                .build();
        MultiFieldQuery query = MultiFieldQuery.start(startTimestamp).end(startTimestamp + SIZE).sub(subQuery).build();
        List<MultiFieldQueryResult> result = tsdb.multiFieldQuery(query);
        if (result != null) {
            System.out.println("##### Multi-field Query Result : " + JSON.toJSONString(result));
            if (result.size() > 0) {
                System.out.println("##### Multi-field Query Result asMap : " + JSON.toJSONString(result.get(0).asMap()));
            }
        } else {
            System.out.println("##### Empty reply from TSDB server. ######");
        }
    }
}
