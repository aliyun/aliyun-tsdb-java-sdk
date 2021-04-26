package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.value.type.QueryType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LastPointQueryTest {
    @Test
    public void testLastPointQuerySerialization() {
        {
            LastPointQuery query = LastPointQuery
                    .builder()
                    .msResolution(true)
                    .tupleFormat(true)
                    .queryType(QueryType.ALL)
                    .sub(LastPointSubQuery.builder("wind1", new ArrayList<String>() {{add("*");}}, new HashMap<String, String>()).build()).build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"msResolution\":true,\"queries\":[{\"metric\":\"wind1\",\"fields\":[\"*\"],\"tags\":{}}],\"tupleFormat\":true,\"type\":\"ALL\"}", serializedString);
        }
        {
            LastPointQuery query = LastPointQuery
                    .builder()
                    .msResolution(true)
                    .tupleFormat(true)
                    .sub(LastPointSubQuery.builder("wind1", new ArrayList<String>() {{add("*");}}, new HashMap<String, String>()).build()).build();
            String serializedString = JSON.toJSONString(query);
            Assert.assertEquals("{\"msResolution\":true,\"queries\":[{\"metric\":\"wind1\",\"fields\":[\"*\"],\"tags\":{}}],\"tupleFormat\":true}", serializedString);
        }
    }

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

    @Test
    public void testLastLimit() {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("k1", "v1");
        LastLimit lastLimit = new LastLimit(1511927280, 2 , true);

        LastPointQuery lastPointQuery = LastPointQuery
                .builder()
                .limit(lastLimit)
                .sub(LastPointSubQuery.builder("m1", tags).build())
                .sub(LastPointSubQuery.builder("m2", tags).build()).build();

        String expected = "{\"limit\":{\"from\":1511927280,\"global\":true,\"size\":2},\"queries\":[{\"metric\":\"m1\",\"tags\":{\"k1\":\"v1\"}},{\"metric\":\"m2\",\"tags\":{\"k1\":\"v1\"}}]}";
        assertEquals(expected, lastPointQuery.toString());
    }

    @Test
    public void testLastRLimit() {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("k1", "v1");
        LastLimit lastLimit = new LastLimit(1511927280, 2 , true);

        LastPointQuery lastPointQuery = LastPointQuery
                .builder()
                .rlimit(2).roffset(2)
                .limit(lastLimit)
                .sub(LastPointSubQuery.builder("m1", tags).build())
                .sub(LastPointSubQuery.builder("m2", tags).build()).build();

        String expected = "{\"limit\":{\"from\":1511927280,\"global\":true,\"size\":2},\"queries\":[{\"metric\":\"m1\",\"tags\":{\"k1\":\"v1\"}},{\"metric\":\"m2\",\"tags\":{\"k1\":\"v1\"}}],\"rlimit\":2,\"roffset\":2}";
        assertEquals(expected, lastPointQuery.toString());
    }

    @Test
    public void testLastSLimit() {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("k1", "v1");
        LastLimit lastLimit = new LastLimit(1511927280, 2 , true);

        LastPointQuery lastPointQuery = LastPointQuery
                .builder()
                .slimit(2)
                .limit(lastLimit)
                .sub(LastPointSubQuery.builder("m1", tags).build())
                .sub(LastPointSubQuery.builder("m2", tags).build()).build();

        String expected = "{\"limit\":{\"from\":1511927280,\"global\":true,\"size\":2},\"queries\":[{\"metric\":\"m1\",\"tags\":{\"k1\":\"v1\"}},{\"metric\":\"m2\",\"tags\":{\"k1\":\"v1\"}}],\"slimit\":2}";
        assertEquals(expected, lastPointQuery.toString());
    }
}
