package com.alibaba.hitsdb.client.http;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.HiTSDB;
import com.alibaba.hitsdb.client.HiTSDBClientFactory;
import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.callback.BatchPutCallback;
import com.alibaba.hitsdb.client.exception.VIPClientException;
import com.alibaba.hitsdb.client.exception.http.HttpClientInitException;
import com.alibaba.hitsdb.client.value.Result;
import com.alibaba.hitsdb.client.value.request.Point;

public class TestHiTSDBRequestTime {
    HiTSDB tsdb;

    @Before
    public void init() throws VIPClientException, HttpClientInitException {
        BatchPutCallback pcb = new BatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
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
                .httpConnectTimeout(5000)
                .config();
        tsdb = HiTSDBClientFactory.connect(config);
    }

    @Test
    public void testLitterDateBatchPutDataCallback() {
        Random random = new Random();
        int time = getTime();
        for (int i = 0; i < 4000; i++) {
            double nextDouble = random.nextDouble() * 100;
            Point point = Point.metric("test1").tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3")
                    .timestamp(time + i).value(nextDouble).build();
            tsdb.put(point);
        }
    }

    @After
    public void after() {
        try {
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

}
