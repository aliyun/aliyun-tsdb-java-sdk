package com.aliyun.hitsdb.client.value.request;

import org.junit.Assert;
import org.junit.Test;

public class MultiPointTest {


	@Test
	public void test_can_set_empty_string_tag_value() {
		MultiFieldPoint multiFieldPoint = MultiFieldPoint.metric("hh").tag("tag", "")
				.timestamp(System.currentTimeMillis())
				.field("h", Math.random()).build();
		Assert.assertNotNull(multiFieldPoint);
		Assert.assertEquals("", multiFieldPoint.getTags().get("tag"));
	}
}
