package com.alibaba.hitsdb.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.value.request.Point;

public class TestHiTSDBClientBatchPutNoLogic {
    private static int getTime() {
        int time;
        try {
            String strDate = "2017-08-16 13:14:15";
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            time = (int) (sdf.parse(strDate).getTime() / 1000);
        } catch (ParseException e) {
            e.printStackTrace();
            time = 0;
        }
        return time;

    }

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
    public void testPutData() {
        Point point = Point.metric("hello").tag("tagk1", "tagv1").timestamp(getTime()).value(123.4).build();
        tsdb.put(point);
    }

    @Test
    public void testLargeDateBatchPutDataCallback() {
        Random random = new Random();
        int time = getTime();
        for(int i = 0;i<100000;i++) {
            double nextDouble = random.nextDouble() * 100;
            Point point = Point.metric("test1")
                                   .tag("tagk1", "tagv1")
                                   .tag("tagk2", "tagv2")
                                   .tag("tagk3", "tagv3")
                                   .timestamp(time + i).value(nextDouble)
                                   .build();
            tsdb.put(point);
        }
    }
    
    @Test
    public void testMiddleDateBatchPutDataCallback() {
        Random random = new Random();
        int time = getTime();
        for(int i = 0;i<5500;i++) {
            double nextDouble = random.nextDouble() * 100;
            Point point = Point.metric("test1")
                                   .tag("tagk1", "tagv1")
                                   .tag("tagk2", "tagv2")
                                   .tag("tagk3", "tagv3")
                                   .timestamp(time + i).value(nextDouble)
                                   .build();
            tsdb.put(point);
        }
    }
    
    @Test
    public void testLitterDateBatchPutDataCallback() {
        Random random = new Random();
        int time = getTime();
        for(int i = 0;i<4000;i++) {
            double nextDouble = random.nextDouble() * 100;
            Point point = Point.metric("test1")
                                   .tag("tagk1", "tagv1")
                                   .tag("tagk2", "tagv2")
                                   .tag("tagk3", "tagv3")
                                   .timestamp(time + i).value(nextDouble)
                                   .build();
            tsdb.put(point);
        }
    }
}
