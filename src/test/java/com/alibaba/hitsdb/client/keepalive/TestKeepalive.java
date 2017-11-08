package com.alibaba.hitsdb.client.keepalive;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.util.UI;
import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.Point;

public class TestKeepalive {
    
    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        UI.pauseStart();
        HiTSDBConfig config = HiTSDBConfig
                .address("127.0.0.1", 8242)
                .httpConnectionPool(1)
                .config();

        tsdb = HiTSDBClientFactory.connect(config);
    }

    @Test
    public void keepalive() throws InterruptedException {
        for (int i = 0; i < 1000000; i++) {
            Point point = createPoint(i % 4, 1.123);
            tsdb.put(point);
            UI.pauseStart();
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
