package com.alibaba.hitsdb.client.callback;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.HiTSDB;
import com.alibaba.hitsdb.client.HiTSDBClientFactory;
import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.exception.VIPClientException;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.util.UI;
import com.alibaba.hitsdb.client.value.request.Point;

public class TestNoLogicCallback {
    HiTSDB tsdb;

    @Before
    public void init() throws VIPClientException, HttpClientInitException {
        UI.pauseStart();
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .httpConnectionPool(64)
                .httpConnectTimeout(2)
                .batchPutRetryCount(3)
                .config();

        tsdb = HiTSDBClientFactory.connect(config);
    }

    @Test
    public void test() throws InterruptedException {
        for (int i = 0; i < 1000000; i++) {
            Point point = createPoint(i % 4, 1.123);
            tsdb.put(point);
            Thread.sleep(1000*2);
        }
    }
    
    @After
    public void end() throws IOException {
        tsdb.close();
    }

    public Point createPoint(int tag, double value) {
        int t = (int) (System.currentTimeMillis() / 1000);
        return Point.metric("test-performance").tag("tag", String.valueOf(tag)).value(t, value).build();
    }
}
