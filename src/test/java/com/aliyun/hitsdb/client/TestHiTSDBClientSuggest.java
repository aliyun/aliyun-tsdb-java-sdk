package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.List;

import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.type.Suggest;

public class TestHiTSDBClientSuggest {
    HiTSDB tsdb;
    
    @Before
    public void init() throws HttpClientInitException {
        tsdb = HiTSDBClientFactory.connect("127.0.0.1", 8242);
        tsdb.putSync(Point.metric("hel-metric-test")
                .tag("hel-tagk-test","hel-tagv-test")
                .value(System.currentTimeMillis(),Math.random())
                .build());
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
    public void testSuggestMetric() {
        List<String> metrics = tsdb.suggest(Suggest.Metrics, "hel", 10);
        System.out.println("查询结果：" + metrics);
    }

    @Test
    public void testSuggestTagK() {
        List<String> metrics = tsdb.suggest(Suggest.Tagk, "hel", 10);
        System.out.println("查询结果：" + metrics);
    }

    @Test
    public void testSuggestTagV() {
        List<String> metrics = tsdb.suggest(Suggest.Tagv, "hel", 10);
        System.out.println("查询结果：" + metrics);
    }
    
}
