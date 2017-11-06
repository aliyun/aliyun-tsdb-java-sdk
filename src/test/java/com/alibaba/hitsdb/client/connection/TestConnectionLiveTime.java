package com.alibaba.hitsdb.client.connection;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.HiTSDB;
import com.alibaba.hitsdb.client.HiTSDBClientFactory;
import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.callback.BatchPutCallback;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.util.UI;
import com.alibaba.hitsdb.client.value.Result;
import com.alibaba.hitsdb.client.value.request.Point;

public class TestConnectionLiveTime {
    
    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        UI.pauseStart();
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .httpConnectionPool(1)
                .batchPutSize(100)
                .listenBatchPut(new BatchPutCallback() {
                    final AtomicLong num = new AtomicLong();
                    
                    @Override
                    public void response(String address, List<Point> input, Result output) {
                        long afterNum = num.addAndGet(input.size());
                        System.out.println("成功处理" + input.size() + ",已处理" + afterNum);
                    }

                    @Override
                    public void failed(String address, List<Point> input, Exception ex) {
                        ex.printStackTrace();
                        long afterNum = num.addAndGet(input.size());
                        System.out.println("失败处理" + input.size() + ",已处理" + afterNum);
                    }
                })
                .config();

        tsdb = HiTSDBClientFactory.connect(config);
    }

    @Test
    public void test() throws InterruptedException {
        for (int i = 0; i < 1000000; i++) {
            Point point = createPoint(i % 4);
            tsdb.put(point);
            Thread.sleep(50);
        }
    }
    
    @After
    public void end() throws IOException {
        tsdb.close();
    }

    public Point createPoint(int tag) {
        int t = (int) (System.currentTimeMillis() / 1000);
        double random = Math.random();
        double value = Math.round(random*1000)/1000.0;
        return Point.metric("test-performance").tag("tag", String.valueOf(tag)).value(t, value).build();
    }
    
}
