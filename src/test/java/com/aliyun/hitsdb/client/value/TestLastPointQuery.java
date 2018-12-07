package com.aliyun.hitsdb.client.value;

import com.aliyun.hitsdb.client.value.request.LastPointQuery;
import com.aliyun.hitsdb.client.value.request.LastPointSubQuery;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created By hujianhong
 * Date: 2018/12/7
 */
public class TestLastPointQuery {

    @Test
    public void testLastPointQuery() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder().backScan(0)
                .msResolution(false)
                .timestamp(1544171825586L)
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());

        assertEquals(query.toJSON(),"{\"backScan\":0,\"msResolution\":false,\"queries\":[{\"tags\":{\"t1\":\"v1\"},\"metric\":\"metric1\"}],\"timestamp\":1544171825586}");
    }

    @Test
    public void testLastPointQuery1() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder()
                .msResolution(false)
                .timestamp(1544171825461L)
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());

        assertEquals(query.toJSON(),"{\"msResolution\":false,\"queries\":[{\"tags\":{\"t1\":\"v1\"},\"metric\":\"metric1\"}],\"timestamp\":1544171825461}");
    }

    @Test
    public void testLastPointQuery2() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder()
                .timestamp(1544171825585L)
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());
        assertEquals(query.toJSON(),"{\"queries\":[{\"tags\":{\"t1\":\"v1\"},\"metric\":\"metric1\"}],\"timestamp\":1544171825585}");
    }

    @Test
    public void testLastPointQuery3() {
        Map<String,String> tag = new HashMap<String, String>();
        tag.put("t1","v1");
        LastPointQuery query = LastPointQuery.builder()
                .sub(LastPointSubQuery.builder("metric1",tag).build()).build();
        System.out.println(query.toJSON());
        assertEquals(query.toJSON(),"{\"queries\":[{\"tags\":{\"t1\":\"v1\"},\"metric\":\"metric1\"}]}");
    }
}
