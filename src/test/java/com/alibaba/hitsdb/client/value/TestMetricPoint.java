package com.alibaba.hitsdb.client.value;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.hitsdb.client.value.request.Point;
import com.alibaba.hitsdb.client.value.type.Granularity;

public class TestMetricPoint {
    
    @Test
    public void testBuild() {
        String metric = "test";
        Point.MetricBuilder pointBuilder = Point.metric(metric);
        Assert.assertNotNull(pointBuilder);
    }

    @Test
    public void testBuildTags() {
        String metric = "test";
        Point.MetricBuilder pointBuilder = Point.metric(metric);
        pointBuilder.tag("tagk1", "tagv1");
        pointBuilder.tag("tagk2", "tagv2");
        pointBuilder.tag("tagk3", "tagv3");
    }

    @Test
    public void testBuildGranularity_S1() {
        String metric = "test";
        Point.MetricBuilder pointBuilder = Point.metric(metric);
        pointBuilder.tag("tagk1", "tagv1");
        pointBuilder.tag("tagk2", "tagv2");
        pointBuilder.tag("tagk3", "tagv3");
        pointBuilder.granularity(Granularity.S1);
        Point point = pointBuilder.build();
        String json = point.toJSON();
        Assert.assertEquals(json, "{\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\",\"tagk3\":\"tagv3\"},\"timestamp\":0}");
        Granularity granularityType = point.getGranularityType();
        Assert.assertEquals(granularityType,Granularity.S1);
        Assert.assertEquals(granularityType.getName(),"1s");
    }
    
    @Test
    public void testBuildGranularity_H1() {
        String metric = "test";
        Point.MetricBuilder pointBuilder = Point.metric(metric);
        pointBuilder.tag("tagk1", "tagv1");
        pointBuilder.tag("tagk2", "tagv2");
        pointBuilder.tag("tagk3", "tagv3");
        pointBuilder.granularity(Granularity.H1);
        Point point = pointBuilder.build();
        String json = point.toJSON();
        Assert.assertEquals(json, "{\"granularity\":\"1h\",\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\",\"tagk3\":\"tagv3\"},\"timestamp\":0}");
        Granularity granularityType = point.getGranularityType();
        Assert.assertEquals(granularityType, Granularity.H1);
        Assert.assertEquals(granularityType.getName(),"1h");
    }
    
    @Test
    public void testBuildTagMap() {
        String metric = "test";
        Point.MetricBuilder pointBuilder = Point.metric(metric);
        pointBuilder.tag("tagk1", "tagv1");
        Map<String, String> map = new HashMap<String, String>();
        map.put("tagk2", "tagv2");
        map.put("tagk3", "tagv3");
        pointBuilder.tag(map);
        Point point = pointBuilder.build();
        String json = point.toJSON();
        Assert.assertEquals(json,
                "{\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\",\"tagk3\":\"tagv3\"},\"timestamp\":0}");
    }

    @Test
    public void testPoint() throws ParseException {
        String metric = "test";
        
        String strDate = "2017-08-01 13:14:15";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sdf.parse(strDate).getTime();

        int timestamp = (int) (time / 1000);
        Point point = Point.metric(metric).tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3")
                .timestamp(timestamp).value(12.3).build();

        String json = point.toJSON();
        Assert.assertEquals(json,
                "{\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\",\"tagk3\":\"tagv3\"},\"timestamp\":1501564455,\"value\":12.3}");
    }
    
    @Test
    public void testPointVersion() throws ParseException {
        String metric = "test";
        long version = 1508123847977l;
        String strDate = "2017-08-01 13:14:15";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sdf.parse(strDate).getTime();

        int timestamp = (int) (time / 1000);
        Point point = Point.metric(metric).tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3")
                .timestamp(timestamp).value(12.3)
                .version(version)
                .build();

        String json = point.toJSON();
        Assert.assertEquals(json,
                "{\"metric\":\"test\",\"tags\":{\"tagk1\":\"tagv1\",\"tagk2\":\"tagv2\",\"tagk3\":\"tagv3\"},\"timestamp\":1501564455,\"value\":12.3,\"version\":1508123847977}");
    }
}
