package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.util.DateUtils;

public class TestHiTSDBClientDeleteData {
    HiTSDB tsdb;
    
    @Before
    public void init() throws HttpClientInitException {
        tsdb = HiTSDBClientFactory.connect("127.0.0.1", 8242);
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
    public void testDeleteData() {
        Date now = DateUtils.now();
        Date start = DateUtils.add(now, - TimeUnit.DAYS.toMillis(10));
        tsdb.deleteData("hello", start, now);;
    }
    
    @Test
    public void testDeleteDataForTime() {
        Date now = DateUtils.now();
        Date start = DateUtils.add(now, - TimeUnit.DAYS.toMillis(10));
        int nowTime = DateUtils.toTimestampSecond(now);
        int startTime = DateUtils.toTimestampSecond(start);
        tsdb.deleteData("hello", nowTime, startTime);;
    }
    
}
