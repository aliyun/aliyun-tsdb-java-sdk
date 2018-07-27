package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHiTSDBClientSingleTimePointQuery {
    // timestamp(second) -> Point
    Map<Long, Point> pointsMap;
    final int dataSize = 100;
    final String metric = "test-single-time-point";
    final String tagk1 = "test-tagk1";
    final String tagk2 = "test-tagk2";
    final String tagv1 = "test-tagv1";
    final String tagv2 = "test-tagv2";

    private void generateDataPoints() {
        if(pointsMap == null) {
            pointsMap = new HashMap<>();
        } else {
            pointsMap.clear();
        }
        long baseTimeInSecond = System.currentTimeMillis() / 1000;
        for(int i = 0; i < dataSize; i++) {
            long ts = baseTimeInSecond + i;
            Point point = Point
                    .metric(metric)
                    .tag(tagk1, tagv1)
                    .tag(tagk2,tagv2)
                    .timestamp(ts)
                    .value(ts)
                    .build();
            pointsMap.put(ts, point);
        }
    }

    private void putData() {
        for(Map.Entry<Long, Point> entry : pointsMap.entrySet()) {
            tsdb.putSync(entry.getValue());
        }
    }

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        pointsMap = new HashMap<>();
        generateDataPoints();
        BatchPutCallback pcb = new BatchPutCallback(){

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
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

        putData();
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
    public void testQuery() {
        for(Map.Entry<Long, Point> entry : pointsMap.entrySet()) {
            long timestamp = entry.getKey();
            Query query = Query
                    .timeRange(timestamp, timestamp)
                    .sub(SubQuery
                            .metric(metric)
                            .aggregator(Aggregator.NONE)
                            .tag(tagk1, tagv1)
                            .tag(tagk2,tagv2)
                            .build())
                    .build();
            List<QueryResult> result = tsdb.query(query);
            Assert.assertEquals(1, result.size());
            Assert.assertEquals(timestamp, ((BigDecimal)result.get(0).getDps().get(timestamp)).longValue());
        }
    }
}
