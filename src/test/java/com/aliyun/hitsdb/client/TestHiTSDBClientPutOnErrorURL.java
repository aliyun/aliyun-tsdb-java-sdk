package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Ignore
public class TestHiTSDBClientPutOnErrorURL {

    HiTSDB tsdb;

    @Before
    public void init() throws  HttpClientInitException {
        // 错误的地址
        // hitsdb.com
        // www.baidu.com
//        HiTSDBConfig config = HiTSDBConfig.address("hitsdb.wwww", 8242).config();
        HiTSDBConfig config = HiTSDBConfig.address("www.baidu.com", 8242)
                .httpConnectTimeout(2)
                .config();
        tsdb = HiTSDBClientFactory.connect(config);
        System.out.println("------------");
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

    @Test
    public void testPutData() throws InterruptedException {
        for (int i = 0; i < 1; i++) {
            Point point = Point.metric("test-test-test").tag("test", "test")
                    .timestamp(System.currentTimeMillis())
                    .value(System.currentTimeMillis())
                    .build();
            tsdb.put(point);
        }

    }
}