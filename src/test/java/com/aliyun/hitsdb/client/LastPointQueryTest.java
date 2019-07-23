package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.value.request.LastPointQuery;
import com.aliyun.hitsdb.client.value.request.LastPointSubQuery;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LastPointQueryTest {

    @Test
    public void testNotCircularReference() {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("k1", "v1");

        LastPointQuery lastPointQuery = LastPointQuery
                .builder()
                .sub(LastPointSubQuery.builder("m1", tags).build())
                .sub(LastPointSubQuery.builder("m2", tags).build()).build();

        String expected = "{\"queries\":[{\"metric\":\"m1\",\"tags\":{\"k1\":\"v1\"}},{\"metric\":\"m2\",\"tags\":{\"k1\":\"v1\"}}]}";
        assertEquals(expected, lastPointQuery.toString());
    }
}
