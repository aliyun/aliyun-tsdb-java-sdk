package com.aliyun.hitsdb.client.value;

import com.aliyun.hitsdb.client.value.request.LastPointQuery;
import com.aliyun.hitsdb.client.value.request.LastPointSubQuery;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

/**
 * Created By hujianhong
 * Date: 2018/12/7
 */
public class TestLastPointQuery {
    private static TimeZone defaultTz;

    @BeforeClass
    public static void setup() {
        defaultTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8:00"));
    }

    @AfterClass
    public static void finish() {
        // reset
        TimeZone.setDefault(defaultTz);
    }

    @Test
    public void testLastPointQuery() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder().backScan(0)
                .msResolution(false)
                .timestamp(1544171825586L)
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());

        assertEquals("{\"backScan\":0,\"msResolution\":false,\"queries\":[{\"metric\":\"metric1\",\"tags\":{\"t1\":\"v1\"}}],\"timestamp\":1544171825586}", query.toJSON());
    }

    @Test
    public void testLastPointQuery1() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder()
                .msResolution(false)
                .timestamp(1544171825461L)
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());

        assertEquals("{\"msResolution\":false,\"queries\":[{\"metric\":\"metric1\",\"tags\":{\"t1\":\"v1\"}}],\"timestamp\":1544171825461}", query.toJSON());
    }

    @Test
    public void testLastPointQuery2() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder()
                .timestamp(1544171825585L)
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());
        assertEquals("{\"queries\":[{\"metric\":\"metric1\",\"tags\":{\"t1\":\"v1\"}}],\"timestamp\":1544171825585}", query.toJSON());
    }

    @Test
    public void testLastPointQuery3() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder()
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());
        assertEquals("{\"queries\":[{\"metric\":\"metric1\",\"tags\":{\"t1\":\"v1\"}}]}", query.toJSON());
    }
}
