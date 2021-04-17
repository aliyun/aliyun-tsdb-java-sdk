package com.aliyun.hitsdb.client.value.request;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author cuiyuan
 * @date 2021/2/10 3:56 下午
 */
public class UniqueUtilTest {

    @Test
    public void testPointHash() {
        Map<String, String> tagMap1 = new HashMap<String, String>();
        tagMap1.put("key1", "value1");
        tagMap1.put("key2", "value2");
        Point point1 = Point.metric("123").timestamp(4294968L).tag(tagMap1).value(1).build();


        Map<String, String> tagMap2 = new HashMap<String, String>();
        tagMap2.put("key2", "value2");
        tagMap2.put("key1", "value1");
        Point point2 = Point.metric("123").timestamp(4294968L).tag(tagMap2).value(2).build();

        Assert.assertEquals(UniqueUtil.hash(point1), UniqueUtil.hash(point2));

        Assert.assertTrue(UniqueUtil.tagsSame(tagMap1, tagMap2));

        tagMap2.put("key1", "value11");
        point2 = Point.metric("123").timestamp(4294968L).tag(tagMap2).value(2).build();
        Assert.assertNotEquals(UniqueUtil.hash(point1), UniqueUtil.hash(point2));

        tagMap2.put("key1", "value1");
        point2 = Point.metric("123").timestamp(4294968L).tag(tagMap2).value(2).build();
        Assert.assertEquals(UniqueUtil.hash(point1), UniqueUtil.hash(point2));

        tagMap2.put("key3", "value3");
        point2 = Point.metric("123").timestamp(4294968L).tag(tagMap2).value(2).build();
        Assert.assertNotEquals(UniqueUtil.hash(point1), UniqueUtil.hash(point2));
        tagMap2.remove("key3");

        point2 = Point.metric("1234").timestamp(4294968L).tag(tagMap2).value(2).build();
        Assert.assertNotEquals(UniqueUtil.hash(point1), UniqueUtil.hash(point2));

        point2 = Point.metric("123").timestamp(14294968L).tag(tagMap2).value(2).build();
        Assert.assertNotEquals(UniqueUtil.hash(point1), UniqueUtil.hash(point2));
    }

    @Test
    public void testMultiFieldHash() {
        Map<String, String> tagMap1 = new HashMap<String, String>();
        tagMap1.put("key1", "value1");
        tagMap1.put("key2", "value2");

        Map<String, Object> fieldMap1 = new HashMap<String, Object>();
        fieldMap1.put("field1", 2);
        fieldMap1.put("field2", 2);

        MultiFieldPoint multiFieldPoint1 = MultiFieldPoint.metric("123").tags(tagMap1).fields(fieldMap1).timestamp(4294968L).build();

        Map<String, String> tagMap2 = new HashMap<String, String>();
        tagMap2.put("key1", "value1");
        tagMap2.put("key2", "value2");

        Map<String, Object> fieldMap2 = new HashMap<String, Object>();
        fieldMap2.put("field1", 1);
        fieldMap2.put("field3", 3);

        MultiFieldPoint multiFieldPoint2 = MultiFieldPoint.metric("123").tags(tagMap2).fields(fieldMap2).timestamp(4294968L).build();

        Assert.assertEquals(UniqueUtil.hash(multiFieldPoint1, "field1"), UniqueUtil.hash(multiFieldPoint2, "field1"));
        Assert.assertNotEquals(UniqueUtil.hash(multiFieldPoint1, "field1"), UniqueUtil.hash(multiFieldPoint2, "field2"));

        multiFieldPoint2 = MultiFieldPoint.metric("1234").tags(tagMap2).fields(fieldMap2).timestamp(4294968L).build();
        Assert.assertNotEquals(UniqueUtil.hash(multiFieldPoint1, "field1"), UniqueUtil.hash(multiFieldPoint2, "field1"));

        tagMap2.put("key3", "value3");
        multiFieldPoint2 = MultiFieldPoint.metric("1234").tags(tagMap2).fields(fieldMap2).timestamp(4294968L).build();
        Assert.assertNotEquals(UniqueUtil.hash(multiFieldPoint1, "field1"), UniqueUtil.hash(multiFieldPoint2, "field1"));
        tagMap2.remove("key3");

        multiFieldPoint2 = MultiFieldPoint.metric("1234").tags(tagMap2).fields(fieldMap2).timestamp(14294968L).build();
        Assert.assertNotEquals(UniqueUtil.hash(multiFieldPoint1, "field1"), UniqueUtil.hash(multiFieldPoint2, "field1"));
    }

    @Test
    public void testUniquePoint() {
        Map<String, String> tagMap1 = new HashMap<String, String>();
        tagMap1.put("key1", "value1");
        tagMap1.put("key2", "value2");
        Point point1 = Point.metric("123").timestamp(4294968L).tag(tagMap1).value(1).build();


        Map<String, String> tagMap2 = new HashMap<String, String>();
        tagMap2.put("key2", "value2");
        tagMap2.put("key1", "value1");
        Point point2 = Point.metric("123").timestamp(4294968L).tag(tagMap2).value(2).build();

        List<Point> points = new ArrayList<Point>();
        points.add(point1);
        points.add(point2);

        Assert.assertTrue(UniqueUtil.pointSame(point1, point2));
        UniqueUtil.uniquePoints(points, true);

        Assert.assertEquals(1, points.size());
        Assert.assertTrue(points.contains(point1));
        Assert.assertFalse(points.contains(point2));
    }

    @Test
    public void testUniqueMultiFieldPoint() {
        Map<String, String> tagMap1 = new HashMap<String, String>();
        tagMap1.put("key1", "value1");
        tagMap1.put("key2", "value2");

        Map<String, Object> fieldMap1 = new HashMap<String, Object>();
        fieldMap1.put("field1", 1);
        fieldMap1.put("field2", 2);

        MultiFieldPoint multiFieldPoint1 = MultiFieldPoint.metric("123").tags(tagMap1).fields(fieldMap1).timestamp(4294968L).build();

        Map<String, String> tagMap2 = new HashMap<String, String>();
        tagMap2.put("key1", "value1");
        tagMap2.put("key2", "value2");

        Map<String, Object> fieldMap2 = new HashMap<String, Object>();
        fieldMap2.put("field1", 2);
        fieldMap2.put("field3", 3);

        MultiFieldPoint multiFieldPoint2 = MultiFieldPoint.metric("123").tags(tagMap2).fields(fieldMap2).timestamp(4294968L).build();

        Assert.assertTrue(UniqueUtil.tagsSame(tagMap1, tagMap2));
        Assert.assertTrue(UniqueUtil.multiFieldPointSame(multiFieldPoint1, multiFieldPoint2, "field1"));

        List<MultiFieldPoint> multiFieldPointList = new ArrayList<MultiFieldPoint>();
        multiFieldPointList.add(multiFieldPoint1);
        multiFieldPointList.add(multiFieldPoint2);

        UniqueUtil.uniqueMultiFieldPoints(multiFieldPointList, true);
        Assert.assertEquals(2, multiFieldPointList.size());
        Assert.assertEquals(2, multiFieldPoint1.getFields().size());
        Assert.assertEquals(1, multiFieldPoint2.getFields().size());
        Assert.assertTrue(multiFieldPoint1.getFields().containsKey("field1"));
        Assert.assertFalse(multiFieldPoint2.getFields().containsKey("field1"));

        tagMap2.put("1", "2");
        Assert.assertFalse(UniqueUtil.tagsSame(tagMap1, tagMap2));
    }

    @Test
    public void testUniqueMultiFieldPoint2() {
        Map<String, String> tagMap1 = new HashMap<String, String>();
        tagMap1.put("key1", "value1");
        tagMap1.put("key2", "value2");

        Map<String, Object> fieldMap1 = new HashMap<String, Object>();
        fieldMap1.put("field1", 1);
        fieldMap1.put("field2", 2);

        MultiFieldPoint multiFieldPoint1 = MultiFieldPoint.metric("123").tags(tagMap1).fields(fieldMap1).timestamp(4294968L).build();

        Map<String, String> tagMap2 = new HashMap<String, String>();
        tagMap2.put("key1", "value1");
        tagMap2.put("key2", "value2");

        Map<String, Object> fieldMap2 = new HashMap<String, Object>();
        fieldMap2.put("field1", 2);
        fieldMap2.put("field2", 3);

        MultiFieldPoint multiFieldPoint2 = MultiFieldPoint.metric("123").tags(tagMap2).fields(fieldMap2).timestamp(4294968L).build();

        List<MultiFieldPoint> multiFieldPointList = new ArrayList<MultiFieldPoint>();
        multiFieldPointList.add(multiFieldPoint1);
        multiFieldPointList.add(multiFieldPoint2);

        UniqueUtil.uniqueMultiFieldPoints(multiFieldPointList, true);
        Assert.assertEquals(1, multiFieldPointList.size());
        Assert.assertTrue(multiFieldPointList.contains(multiFieldPoint1));
        Assert.assertTrue(multiFieldPoint1.getFields().containsKey("field1") && multiFieldPoint1.getFields().get("field1").toString().equals("1"));
        Assert.assertTrue(multiFieldPoint1.getFields().containsKey("field2") && multiFieldPoint1.getFields().get("field2").toString().equals("2"));
        Assert.assertFalse(multiFieldPointList.contains(multiFieldPoint2));
        Assert.assertEquals(0, multiFieldPoint2.getFields().size());
    }

    @Test
    public void testUniqueMultiFieldPoint3() {
        Map<String, String> tagMap1 = new HashMap<String, String>();
        tagMap1.put("key1", "value11");
        tagMap1.put("key2", "value2");

        Map<String, Object> fieldMap1 = new HashMap<String, Object>();
        fieldMap1.put("field1", 1);
        fieldMap1.put("field2", 2);

        MultiFieldPoint multiFieldPoint1 = MultiFieldPoint.metric("123").tags(tagMap1).fields(fieldMap1).timestamp(4294968L).build();

        Map<String, String> tagMap2 = new HashMap<String, String>();
        tagMap2.put("key1", "value1");
        tagMap2.put("key2", "value2");

        Map<String, Object> fieldMap2 = new HashMap<String, Object>();
        fieldMap2.put("field1", 2);
        fieldMap2.put("field2", 3);

        MultiFieldPoint multiFieldPoint2 = MultiFieldPoint.metric("123").tags(tagMap2).fields(fieldMap2).timestamp(4294968L).build();

        List<MultiFieldPoint> multiFieldPointList = new ArrayList<MultiFieldPoint>();
        multiFieldPointList.add(multiFieldPoint1);
        multiFieldPointList.add(multiFieldPoint2);

        UniqueUtil.uniqueMultiFieldPoints(multiFieldPointList, true);
        Assert.assertEquals(2, multiFieldPointList.size());
        Assert.assertTrue(multiFieldPointList.contains(multiFieldPoint1));
        Assert.assertTrue(multiFieldPointList.contains(multiFieldPoint2));
        Assert.assertEquals(2, multiFieldPoint1.getFields().size());
        Assert.assertEquals(2, multiFieldPoint2.getFields().size());
        Assert.assertTrue(multiFieldPoint1.getFields().containsKey("field1") && multiFieldPoint1.getFields().get("field1").toString().equals("1"));
        Assert.assertTrue(multiFieldPoint1.getFields().containsKey("field2") && multiFieldPoint1.getFields().get("field2").toString().equals("2"));
        Assert.assertTrue(multiFieldPoint2.getFields().containsKey("field1") && multiFieldPoint2.getFields().get("field1").toString().equals("2"));
        Assert.assertTrue(multiFieldPoint2.getFields().containsKey("field2") && multiFieldPoint2.getFields().get("field2").toString().equals("3"));
    }

    @Test
    public void testMultiFieldPointHashCollision() {

        Map<String, Object> fieldMap1 = new HashMap<String, Object>();
        fieldMap1.put("WorkSwitch", 1);
        fieldMap1.put("Mode", 2);

        Map<String, Object> fieldMap2 = new HashMap<String, Object>();
        fieldMap2.put("Switch", 2);
        fieldMap2.put("DefrostMode", 3);

        MultiFieldPoint point1 = MultiFieldPoint.metric("31property/g0dsPke7EBf").tag("_id", "6737531").timestamp(1515859201).fields(fieldMap1).build();
        MultiFieldPoint point2 = MultiFieldPoint.metric("6property/g0dsPke7EBf").tag("_id", "4937406").timestamp(1515859201).fields(fieldMap2).build();

         Assert.assertEquals(UniqueUtil.hash(point1, "WorkSwitch"), UniqueUtil.hash(point2, "DefrostMode") ); //产生了hash碰撞

         List<MultiFieldPoint> multiFieldPoints = new ArrayList<MultiFieldPoint>();
         multiFieldPoints.add(point1);
         multiFieldPoints.add(point2);

        UniqueUtil.uniqueMultiFieldPoints(multiFieldPoints, true);
        //并不应该去重
        Assert.assertEquals(2, multiFieldPoints.size());
        Assert.assertEquals(2, point1.getFields().size());
        Assert.assertEquals(2, point2.getFields().size());
    }
}
