package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.LastDataValue;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Timeline;

public class TestHiTSDBClientQueryLast {
    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig.address("localhost", 8242).config();
        tsdb = HiTSDBClientFactory.connect(config);
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
    public void testQueryLast() throws InterruptedException {
        long timeStamp = 1501379000;
        // Double Data Type
        tsdb.put(Point.metric("test_metric").tag("datatype", "double").value(timeStamp, 123.4).build(),
                Point.metric("test_metric").tag("datatype", "double").value(timeStamp-1, 567.8).build(),
                Point.metric("test_metric").tag("datatype", "double").value(timeStamp+1, 910.11).build(),
                Point.metric("test_metric").tag("datatype", "double").value(timeStamp-2, 555.55).build());

        // String Data Type
        tsdb.put(Point.metric("test_metric").tag("datatype", "string").value(timeStamp, "string_1").build(),
                Point.metric("test_metric").tag("datatype", "string").value(timeStamp-1, "string_2").build(),
                Point.metric("test_metric").tag("datatype", "string").value(timeStamp+1, "string_3").build(),
                Point.metric("test_metric").tag("datatype", "string").value(timeStamp-2, "string_4").build());

        // Boolean Data Type
        tsdb.put(Point.metric("test_metric").tag("datatype", "boolean").value(timeStamp, true).build(),
                Point.metric("test_metric").tag("datatype", "boolean").value(timeStamp-1, false).build(),
                Point.metric("test_metric").tag("datatype", "boolean").value(timeStamp+1, false).build(),
                Point.metric("test_metric").tag("datatype", "boolean").value(timeStamp-2, true).build());

        // Integer Data Type
        tsdb.put(Point.metric("test_metric").tag("datatype", "integer").value(timeStamp, 123).build(),
                Point.metric("test_metric").tag("datatype", "integer").value(timeStamp-1, 456).build(),
                Point.metric("test_metric").tag("datatype", "integer").value(timeStamp+1, 789).build(),
                Point.metric("test_metric").tag("datatype", "integer").value(timeStamp-2, 123456).build());

        Thread.sleep(3000);

        Timeline doubleType = Timeline.metric("test_metric").tag("datatype", "double").build();
        Timeline stringType = Timeline.metric("test_metric").tag("datatype", "string").build();
        Timeline booleanType = Timeline.metric("test_metric").tag("datatype", "boolean").build();
        Timeline integerType = Timeline.metric("test_metric").tag("datatype", "integer").build();
        List<LastDataValue> lastDataValues = tsdb.queryLast(Arrays.asList(doubleType, stringType, booleanType, integerType));
        System.out.println("Last Data Values: " + lastDataValues);

        List<String> tsuids = new LinkedList<String>();
        for (LastDataValue lastDataValue : lastDataValues) {
            String tsuid = lastDataValue.getTsuid();
            tsuids.add(tsuid);
        }

        lastDataValues = tsdb.queryLast(tsuids);
        System.out.println("Last Data Values: " + lastDataValues);

        // Test Query
        Query query = Query
                .timeRange(timeStamp-2, timeStamp+1)
                .sub(SubQuery.metric("test_metric").aggregator(Aggregator.NONE).tag("datatype", "*").build())
                .build();

        List<QueryResult> result = tsdb.query(query);
        // QueryResult queryResult = result.get(0);
        // LinkedHashMap<Long, Object> dps = queryResult.getDps();
        System.out.println("查询结果：" + result);
    }
}
