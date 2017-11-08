package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;

public class TestHiTSDBClientBatchPut {

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
        BatchPutCallback pcb = new BatchPutCallback(){
            
            final AtomicInteger num = new AtomicInteger();
            
            @Override
            public void failed(String address,List<Point> points,Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("已处理" + count + "个点");
            }
            
        };
        
        HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 8242)
                .listenBatchPut(pcb)
                .httpConnectTimeout(3)
                .batchPutRetryCount(2)
                .config();
        tsdb = HiTSDBClientFactory.connect(config);
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
    public void testPutData() {
    		int t = (int) (1508742134297l/1000);  // 1508742134
    		int t1 = t - 1;
        Point point = Point.metric("test-test-test").tag("level", "500")
                .timestamp(t1)
                .value(123.4567)
                .build();
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
