package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.aliyun.hitsdb.client.value.request.GeoPointValue;
import com.aliyun.hitsdb.client.value.request.ValueSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author cuiyuan
 * @date 2020/8/5 10:33 上午
 */
public class TestMultiFieldQueryResult {
    @Test
    public void testMultiFieldQueryResultSerialize() {
        MultiFieldQueryResult multiFieldQueryResult = new MultiFieldQueryResult();
        multiFieldQueryResult.setMetric("hello");
        List<List<Object>> values  = new ArrayList<List<Object>>();
        List<Object> value = new ArrayList<Object>();
        final byte[] bytes = new byte[]{0x01,0x02,0x03};
        value.add(2);
        value.add(bytes);
        values.add(value);
        multiFieldQueryResult.setValues(values);
        System.out.println(multiFieldQueryResult);

        String jsonString = JSON.toJSONString(multiFieldQueryResult);
        System.out.println(jsonString);

        MultiFieldQueryResult multiFieldQueryResult1 = JSON.parseObject(jsonString, MultiFieldQueryResult.class);

        System.out.println(multiFieldQueryResult);
        System.out.println(multiFieldQueryResult1);
        Assert.assertArrayEquals((byte[]) multiFieldQueryResult1.getValues().get(0).get(1),bytes);
        Assert.assertArrayEquals((byte[]) multiFieldQueryResult1.getValues().get(0).get(1),(byte[]) multiFieldQueryResult.getValues().get(0).get(1));
    }

    @Test
    public void testMultiFieldQueryGeoPointResultSerialize() {
        MultiFieldQueryResult multiFieldQueryResult = new MultiFieldQueryResult();
        multiFieldQueryResult.setMetric("hello");
        List<List<Object>> values  = new ArrayList<List<Object>>();
        List<Object> value = new ArrayList<Object>();
        final GeoPointValue gp = new GeoPointValue("POINT (111 22)");
        value.add(2);
        value.add(gp);
        values.add(value);
        multiFieldQueryResult.setValues(values);
        System.out.println(multiFieldQueryResult);

        String jsonString = JSON.toJSONString(multiFieldQueryResult);
        System.out.println(jsonString);

        MultiFieldQueryResult multiFieldQueryResult1 = JSON.parseObject(jsonString, MultiFieldQueryResult.class);

        System.out.println(multiFieldQueryResult);
        System.out.println(multiFieldQueryResult1);
        Assert.assertEquals(multiFieldQueryResult1.getValues().get(0).get(1), gp);
        Assert.assertEquals(multiFieldQueryResult1.getValues().get(0).get(1), multiFieldQueryResult.getValues().get(0).get(1));
    }
}
