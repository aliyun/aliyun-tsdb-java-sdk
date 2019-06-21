package com.aliyun.hitsdb.client.value.request;

import org.junit.Assert;
import org.junit.Test;

public class PointTest {


	@Test
	public void test_can_set_empty_string_tag_value() {
		Point point = Point.metric("hh").tag("tag", "").value(System.currentTimeMillis(), Math.random()).build();
		Assert.assertNotNull(point);
		Assert.assertEquals("", point.getTags().get("tag"));
	}
}
