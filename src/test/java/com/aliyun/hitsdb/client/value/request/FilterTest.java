package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.type.FilterType;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class FilterTest {


    @Test
    public void testFilterBuilderWithoutGroupBy() {
        Filter filter = Filter.filter(FilterType.Wildcard,"*").build();
        assertNotNull(filter);
    }


    @Test
    public void testFilterBuilderWithGroupBy() {
        Filter filter = Filter.filter(FilterType.Wildcard,"tagk","null",false).build();
        assertNotNull(filter);
    }

    @Test
    public void testGeoFilterBuilder() {
        Filter filter = Filter.filter(FilterType.GeoIntersects,"tagk","POLYGON ((111 22, 111 23, 111 24, 111 22))",false).build();
        assertNotNull(filter);
    }
}
