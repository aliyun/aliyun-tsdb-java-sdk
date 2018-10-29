package com.aliyun.hitsdb.client.balance;

import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created By jianhong.hjh
 * Date: 2018/10/27
 */
public class TestPoint {

    @Test
    public void testEmpty() {
        Map<String,String> tags = new HashMap<String, String>();
        tags.put("hh","");
        tags.put("test","");
        Point point = new Point.MetricBuilder("test")
                .tag(tags)
                .value(System.currentTimeMillis(),Math.random())
                .build();
    }

    @Test
    public void testNull() {
        Map<String,String> tags = new HashMap<String, String>();
        tags.put("kk",null);
        tags.put("test","");
        Point point = new Point.MetricBuilder("test")
                .tag(tags)
                .value(System.currentTimeMillis(),Math.random())
                .build();
    }
}
