package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.value.request.ByteArrayValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author cuiyuan
 * @date 2020/8/7 4:17 下午
 */
public class TestQueryResult {

    @Test
    public void testQueryResultSerialize(){
        String jsonString = "[\n" +
                "  {\n" +
                "    \"metric\": \"test1\",\n" +
                "    \"tags\": {\n" +
                "      \"tagk1\": \"tagv1\",\n" +
                "      \"tagk2\": \"tagv2\"\n" +
                "    },\n" +
                "    \"aggregateTags\": [],\n" +
                "    \"dps\": {\n" +
                "      \"1024234234\": {\n" +
                "        \"type\": \"bytes\",\n" +
                "        \"content\": \"dGVzdA==\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "]\n" +
                "\n" +
                "\n";

        List<QueryResult> queryResults = JSON.parseArray(jsonString, QueryResult.class);
        System.out.println(queryResults);
    }

    @Test
    public void testQueryResutlSerialize2() {
        QueryResult queryResult = new QueryResult();
        queryResult.setMetric("hello");
        LinkedHashMap<Long,Object> hashMap = new LinkedHashMap<>();
        final byte[] value = new byte[]{0x01,0x02,0x03};
        hashMap.put(123L, new ByteArrayValue(value));
        queryResult.setDps(hashMap);

        String jsonString = JSON.toJSONString(queryResult);
        System.out.println(jsonString);

        QueryResult queryResult1 = JSON.parseObject(jsonString,QueryResult.class);
        System.out.println(queryResult1);
        Assert.assertArrayEquals(value, (byte[]) queryResult1.getDps().get(123L));
    }
}
