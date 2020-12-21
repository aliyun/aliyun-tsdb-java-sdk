package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.TSDBClientFactory;
import com.aliyun.hitsdb.client.TSDBConfig;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestPutWithTemperaryCallback {
    TSDB tsdb;

    @Before
    public void init() throws HttpClientInitException, IOException {
        System.out.println("开始运行");

        TSDBConfig config = TSDBConfig
                .address("127.0.0.1", 8242)
                .httpConnectionPool(100)
                .config();

        tsdb = TSDBClientFactory.connect(config);
    }

    @Test
    public void test() throws InterruptedException {
        BatchPutSummaryCallback pcb = new BatchPutSummaryCallback() {

            @Override
            public void response(String address, List<Point> input, SummaryResult result) {
                int success = result.getSuccess();
                int failed = result.getFailed();
                System.out.println("write data successfully, success: " + success + ", failure: " + failed);
            }

            @Override
            public void failed(String address, List<Point> input, Exception ex) {
                System.out.println("failed to write points, " + ex.getMessage());
            }

        };

        List<Point> points = new ArrayList<Point>();
        for(int i = 0;i<1000;i++) {
            points.add(createPoint(i%4,1.123));
        }

        tsdb.put(points, pcb);

        tsdb.put(Collections.EMPTY_LIST, pcb);

        Thread.sleep(1000);
    }

    @Test
    public void testMultiField() throws InterruptedException {
        MultiFieldBatchPutDetailsCallback pcb = new MultiFieldBatchPutDetailsCallback() {

            @Override
            public void response(String address, List<MultiFieldPoint> input, MultiFieldDetailsResult result) {
                int success = result.getSuccess();
                int failed = result.getFailed();
                System.out.println("write multi-field data successfully, success: " + success + ", failure: " + failed);
            }

            @Override
            public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                System.out.println("failed to write multi-field points, " + ex.getMessage());
            }

        };

        List<MultiFieldPoint> points = new ArrayList<MultiFieldPoint>();
        for(int i = 0;i<1000;i++) {
            points.add(createMFPoint(i%4,1.123, 4.432));
        }

        tsdb.multiFieldPut(points, pcb);

        MultiFieldBatchPutDetailsCallback pcb2 = new MultiFieldBatchPutDetailsCallback() {

            @Override
            public void response(String address, List<MultiFieldPoint> input, MultiFieldDetailsResult result) {
                System.out.println("empty callback");
            }

            @Override
            public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                System.out.println("nothing to fail");
            }

        };

        tsdb.multiFieldPut(Collections.EMPTY_LIST, pcb2);

        Thread.sleep(1000);
    }

    public Point createPoint(int tag, double value) {
        int t = (int) (System.currentTimeMillis() / 1000);
        return Point.metric("TestPutWithTemperaryCallback").tag("tag", String.valueOf(tag)).value(t, value).build();
    }

    public MultiFieldPoint createMFPoint(int tag, double value1, double value2) {
        int t = (int) (System.currentTimeMillis() / 1000);
        return MultiFieldPoint.metric("TestPutWithTemperaryCallback").tag("tag", String.valueOf(tag))
                .field("f1", value1).field("f2", value2).timestamp(t).build();
    }
}
