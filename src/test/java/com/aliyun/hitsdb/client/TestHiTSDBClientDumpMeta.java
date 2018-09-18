package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.response.TagResult;

public class TestHiTSDBClientDumpMeta {
    HiTSDB tsdb;


    String metricPrefix = "nongfu_spring";

    String tagkey = "address";

    String tagValuePrefix = "china";

    @Before
    public void init() throws HttpClientInitException {
        tsdb = HiTSDBClientFactory.connect("localhost", 3242);

        for(int i = 0;i < 10;i ++){
            for(int j = 0; j < 10;j ++){
                Point point = Point.metric(metricPrefix + i)
                        .tag(tagkey,tagValuePrefix + i + j)
                        .value(new Date(System.currentTimeMillis()),Math.random()).build();
                tsdb.put(point);
            }
        }

        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
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



    @Test
    public void testDumpMeta() {
        List<TagResult> dumpMeta = tsdb.dumpMeta(tagkey, tagValuePrefix, 150);
        assert dumpMeta.size() == 100;
        System.out.println("查询结果：" + dumpMeta.size() + "\t" + dumpMeta);
    }


    @Test
    public void testMetricDumpMeta(){
        for(int i = 0;i < 10;i ++){
            List<TagResult> dumpMeta = tsdb.dumpMeta(metricPrefix + i,tagkey, tagValuePrefix, 20);
            assert dumpMeta.size() == 10;
            System.out.println("查询结果：" + dumpMeta.size() + "\t" + dumpMeta);
        }
    }


    @Test
    public void testMetricDumpMeta2(){
        for(int i = 0;i < 10;i ++){
            List<TagResult> dumpMeta = tsdb.dumpMeta(metricPrefix + i,tagkey, "", 20);
            assert dumpMeta.size() == 10;
            System.out.println("查询结果：" + dumpMeta.size() + "\t" + dumpMeta);
        }
    }

    @Test
    public void testDumpMetric() {
        List<String> dumpMeta = tsdb.dumpMetric(tagkey, tagValuePrefix, 100);
        assert dumpMeta.size() == 10;
        System.out.println("查询结果：" + dumpMeta.size() + "\t" + dumpMeta);
    }
}
