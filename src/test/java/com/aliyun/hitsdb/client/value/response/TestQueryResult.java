package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.value.request.ByteArrayValue;
import com.aliyun.hitsdb.client.value.request.GeoPointValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
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
    public void testQueryResultTimeOrder() {
        String jsonString = "[{\"aggregateTags\":[],\"dps\":{1509589800000:6.0,1509631800000:7.0,1509589200000:5.0,1509637800000:8.0,1509469800000:0.1,1509556200000:4.0,1509507000000:0.3,1509502800000:0.2},\"metric\":\"testBaseQuery\",\"sdps\":{},\"tags\":{\"tgk1\":\"tgv1\"}}]";
        String expectedString = "[{\"aggregateTags\":[],\"dps\":{1509469800000:0.1,1509502800000:0.2,1509507000000:0.3,1509556200000:4.0,1509589200000:5.0,1509589800000:6.0,1509631800000:7.0,1509637800000:8.0},\"metric\":\"testBaseQuery\",\"sdps\":{},\"tags\":{\"tgk1\":\"tgv1\"}}]";
        List<QueryResult> queryResult = JSON.parseArray(jsonString, QueryResult.class);
        Assert.assertEquals(expectedString, wrap(queryResult).toString());

        List<QueryResult> queryResult1 = JSON.parseArray(expectedString, QueryResult.class);
        Assert.assertEquals(expectedString, wrap(queryResult1).toString());
    }

    public static List<CaseQueryResult> wrap(List<QueryResult> queryResults){
        List<CaseQueryResult> results = new ArrayList<CaseQueryResult>(queryResults.size());
        for(int i = 0,size = queryResults.size();i < size;i ++){
            results.add(CaseQueryResult.wrap(queryResults.get(i)));
        }
        return results;
    }

    public static class CaseQueryResult extends QueryResult {
        @Override
        public List<KeyValue> getOrderDps() {
            return null;
        }

        @Override
        public List<KeyValue> getOrderDps(boolean reverse) {
            return null;
        }

        public static CaseQueryResult wrap(QueryResult queryResult) {
            CaseQueryResult result = new CaseQueryResult();
            result.setMetric(queryResult.getMetric());
            result.setTags(queryResult.getTags());
            result.setDps(queryResult.getDps());
            result.setAggregateTags(queryResult.getAggregateTags());
            result.setSdps(queryResult.getSdps());
            return result;
        }
    }

    @Test
    public void testQueryResultWithGeoPointSerialize(){
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
                "        \"type\": \"geopoint\",\n" +
                "        \"content\": \"POINT (111 22)\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "]\n" +
                "\n" +
                "\n";

        List<QueryResult> queryResults = JSON.parseArray(jsonString, QueryResult.class);
        int index = 0;
        for (Map.Entry dp : queryResults.get(0).getDps().entrySet()) {
            Assert.assertEquals(dp.getKey(), 1024234234L);
            Assert.assertEquals(dp.getValue(), new GeoPointValue("POINT (111 22)"));
            Assert.assertEquals(index, 0);
            index++;
        }
    }

    @Test
    public void testQueryResutlSerialize2() {
        QueryResult queryResult = new QueryResult();
        queryResult.setMetric("hello");
        LinkedHashMap<Long,Object> hashMap = new LinkedHashMap<Long,Object>();
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
