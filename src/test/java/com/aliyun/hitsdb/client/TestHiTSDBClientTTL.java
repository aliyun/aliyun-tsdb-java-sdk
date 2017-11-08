package com.aliyun.hitsdb.client;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;

public class TestHiTSDBClientTTL {

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 8242).config();
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
    public void testPostTTL() {
        // 10年
        int time = 2678400;
        tsdb.ttl(time);

        System.out.println("结束");
        try {
            Thread.sleep(100000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetTTL() {
        int ttl = tsdb.ttl();
        System.out.println(ttl);

        try {
            Thread.sleep(100000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
