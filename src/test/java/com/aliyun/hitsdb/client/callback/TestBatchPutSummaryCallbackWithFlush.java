package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.*;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestBatchPutSummaryCallbackWithFlush {
    TSDB tsdb;

    @Before
    public void init() throws HttpClientInitException, IOException {
        
        BatchPutSummaryCallback pcb = new BatchPutSummaryCallback() {

            @Override
            public void response(String address,List<Point> input, SummaryResult result) {
                int success = result.getSuccess();
                int failed = result.getFailed();
                System.out.printf(" %d points success, %d points failed in thread no. [%d]\n", success, failed, Thread.currentThread().getId());
            }
            
        };
        
        TSDBConfig config = TSDBConfig
                .address("127.0.0.1", 8242)
                .httpConnectionPool(100)
                .batchPutConsumerThreadCount(1)
                .ioThreadCount(2)
                .batchPutTimeLimit(200)
                .batchPutSize(400)
                .listenBatchPut(pcb)
                .config();
        
        tsdb = TSDBClientFactory.connect(config);
    }
    
    @Test
    public void test() throws InterruptedException {
        for(int i = 0;i<1000;i++) {
            Point point = createPoint(i%4,1.123);
            tsdb.put(point);
            Thread.sleep(1000);
        }
    }


    @Test
    public void testWithFlush() throws InterruptedException {
        List<List<Point>> pointLists = new ArrayList<List<Point>>();
        List<Point> points1 = new ArrayList<Point>(), points2 = new ArrayList<Point>(), points3 = new ArrayList<Point>();
        pointLists.add(points1);
        pointLists.add(points2);
        pointLists.add(points3);

        Iterator<List<Point>> iterator = pointLists.iterator();
        while (iterator.hasNext()) {
            List<Point> pts = iterator.next();
            for (int i = 0; i < 1000; i++) {
                pts.add(createPoint(i % 4, 1.123));
            }
        }
        tsdb.put(pointLists.get(0));
        tsdb.put(pointLists.get(1));
        tsdb.put(pointLists.get(2));
        // for the performance purpose, flush did COPY the remaining objects out of the queue.
        // as a result, the flushed points might be re-consumed again
        tsdb.flush();

        Thread.sleep(2000);
    }
    
    public Point createPoint(int tag, double value) {
        int t = (int) (System.currentTimeMillis() / 1000);
        return Point.metric("test-performance").tag("tag", String.valueOf(tag)).value(t, value).build();
    }
}
