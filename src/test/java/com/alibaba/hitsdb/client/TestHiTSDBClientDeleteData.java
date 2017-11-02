package com.alibaba.hitsdb.client;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.exception.VIPClientException;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.util.DateUtils;

public class TestHiTSDBClientDeleteData {
    HiTSDB tsdb;
    
    @Before
    public void init() throws VIPClientException, HttpClientInitException {
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
