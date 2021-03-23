package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MultiFieldTest {

	@Test
	public void test_can_set_empty_string_tag_value() {
		MultiFieldPoint multiFieldPoint = MultiFieldPoint.metric("hh").tag("tag", "")
				.timestamp(System.currentTimeMillis())
				.field("h", Math.random()).build();
		Assert.assertNotNull(multiFieldPoint);
		Assert.assertEquals("", multiFieldPoint.getTags().get("tag"));
	}

	@Test
	public void testMultiFieldPointSerialize() {
		MultiFieldPoint point = MultiFieldPoint
				.metric("test-test-test")
				.tag("a", "1")
				.tag("b", "2")
				.timestamp(System.currentTimeMillis())
				.field("f1", Math.random())
				.field("f2", Math.random())
				.build();

		System.out.println(point);
		String jsonString = JSON.toJSONString(point);
		System.out.println(jsonString);
		Assert.assertFalse(jsonString.contains("type"));
		Assert.assertFalse(jsonString.contains("content"));

		final byte[] byteValue = new byte[]{0x1a,0x0a,0x0f};
		point.getFields().put("f3", byteValue);

		System.out.println(point);
		jsonString = JSON.toJSONString(point);
		System.out.println(jsonString);
		System.out.println(point);
		Assert.assertTrue(jsonString.contains("type"));
		Assert.assertTrue(jsonString.contains("content"));

		MultiFieldPoint point1 = JSON.parseObject(jsonString, MultiFieldPoint.class);
		System.out.println(point);
		System.out.println(point1.toString());
		System.out.println(point1.getFields().get("f3"));
		Assert.assertArrayEquals((byte[]) point1.getFields().get("f3"), byteValue);
		Assert.assertArrayEquals((byte[]) point1.getFields().get("f3"), (byte[]) point.getFields().get("f3"));
	}

	@Test
	public void testMultiFieldGeoPointSerialize() {
		MultiFieldPoint point = MultiFieldPoint
				.metric("test-test-test")
				.tag("a", "1")
				.tag("b", "2")
				.timestamp(System.currentTimeMillis())
				.field("f1", Math.random())
				.field("f2", Math.random())
				.build();

		System.out.println(point);
		String jsonString = JSON.toJSONString(point);
		System.out.println(jsonString);
		Assert.assertFalse(jsonString.contains("type"));
		Assert.assertFalse(jsonString.contains("content"));

		final String wktString = "POINT (110 23)";
		final GeoPointValue gp = new GeoPointValue(wktString);
		point.getFields().put("f3", gp);

		System.out.println(point);
		jsonString = JSON.toJSONString(point);
		System.out.println(jsonString);
		System.out.println(point);
		Assert.assertTrue(jsonString.contains("type"));
		Assert.assertTrue(jsonString.contains("content"));

		MultiFieldPoint point1 = JSON.parseObject(jsonString, MultiFieldPoint.class);
		System.out.println(point);
		System.out.println(point1.toString());
		System.out.println(point1.getFields().get("f3"));
		Assert.assertEquals(point1.getFields().get("f3"), gp);
		Assert.assertEquals(point1.getFields().get("f3"), point.getFields().get("f3"));
	}

	@Test
	public void testQueryRLimit() {
		long startTime = 1511927280;
		long endTime = 1511937280;
		String metric = "test-test-test";
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("k1", "v1");

		MultiFieldQuery mq = MultiFieldQuery.start(startTime).end(endTime).sub(
				MultiFieldSubQuery.metric(metric).rlimit(2).roffset(3).limit(4).fieldsInfo(
						MultiFieldSubQueryDetails.aggregator(Aggregator.NONE).field("*").build()
				).build()
		).build();

		String expected = "{\"end\":1511937280,\"queries\":[{\"fields\":[{\"aggregator\":\"none\",\"aggregatorType\":\"NONE\",\"delta\":false,\"field\":\"*\",\"rate\":false,\"top\":0}],\"index\":0,\"limit\":4,\"metric\":\"test-test-test\",\"rlimit\":2,\"roffset\":3}],\"start\":1511927280}";
		assertEquals(expected, mq.toString());
	}
}
