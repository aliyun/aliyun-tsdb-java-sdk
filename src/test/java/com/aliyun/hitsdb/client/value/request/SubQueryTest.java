package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.FilterType;
import org.junit.Assert;
import org.junit.Test;

public class SubQueryTest {




    @Test
    public void testBuildFilter() {
       SubQuery subQuery = SubQuery.metric("mm").aggregator(Aggregator.NONE).tag("a","b").filter(FilterType.Wildcard,"tagk","*").build();
       Assert.assertNotNull(subQuery);
    }
}
