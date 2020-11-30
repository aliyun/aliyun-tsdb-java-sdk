package com.aliyun.hitsdb.client.value;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.*;

import com.aliyun.hitsdb.client.value.request.Point;

public class TestMetricPointStringValue {
    
    private String metric;
    private long time;

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
    
    @Before
    public void init() throws ParseException {
        metric = "test";
        String strDate = "2017-08-01 13:14:15";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        time = sdf.parse(strDate).getTime();
    }
    
    @Test
    public void testPoint0() {
        int timestamp = (int) (time / 1000);
        Point point = Point
                .metric(metric)
                .tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3")
                .timestamp(timestamp)
                .value(12.3)
                .build();

        String json = point.toJSON();
        Assert.assertEquals("{\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\",\"tagk3\":\"tagv3\"},\"timestamp\":1501564455,\"value\":12.3}",
                json);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testPoint1() {
        int timestamp = (int) (time / 1000);
        Point point = Point
                .metric(metric)
                .tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3")
                .timestamp(timestamp)
                .build();
    }
}
