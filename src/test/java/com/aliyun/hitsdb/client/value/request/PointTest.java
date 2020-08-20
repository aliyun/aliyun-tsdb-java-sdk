package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import org.junit.Assert;
import org.junit.Test;

public class PointTest {


	@Test
	public void test_can_set_empty_string_tag_value() {
		Point point = Point.metric("hh").tag("tag", "").value(System.currentTimeMillis(), Math.random()).build();
		Assert.assertNotNull(point);
		Assert.assertEquals("", point.getTags().get("tag"));
	}

	@Test
	public void testPointValueSerialize() {
		String metric = "test";

		final byte[] byteValue = new byte[]{0x1a,0x0a,0x0f};
		Point point = Point.metric(metric).tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3")
				.timestamp(System.currentTimeMillis()).value(byteValue).build();

		String jsonString = JSON.toJSONString(point);
		Assert.assertTrue(jsonString.contains("type"));
		Assert.assertTrue(jsonString.contains("content"));

		Point p = JSON.parseObject(jsonString,Point.class);
		Assert.assertArrayEquals((byte[]) p.getValue(), byteValue);

		point.setValue(2);
		jsonString = JSON.toJSONString(point);
		Assert.assertFalse(jsonString.contains("type"));
		Assert.assertFalse(jsonString.contains("content"));
		p =JSON.parseObject(jsonString,Point.class);
		Assert.assertSame(p.getValue(), 2);
	}

	@Test
	public void testGeoPointValueSerialize() {
		String metric = "test";

		final String wktString = "POINT (110 23)";
		final GeoPointValue gp = new GeoPointValue(wktString);
		Point point = Point.metric(metric).tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3")
				.timestamp(System.currentTimeMillis()).value(gp).build();

		String jsonString = JSON.toJSONString(point);
		Assert.assertTrue(jsonString.contains("type"));
		Assert.assertTrue(jsonString.contains("geopoint"));
		Assert.assertTrue(jsonString.contains("content"));
		Assert.assertTrue(jsonString.contains(wktString));


		Point p = JSON.parseObject(jsonString, Point.class);
		Assert.assertEquals(p.getValue(), gp);

		point.setValue(2);
		jsonString = JSON.toJSONString(point);
		Assert.assertFalse(jsonString.contains("type"));
		Assert.assertFalse(jsonString.contains("content"));
		p =JSON.parseObject(jsonString, Point.class);
		Assert.assertSame(p.getValue(), 2);
	}
}
