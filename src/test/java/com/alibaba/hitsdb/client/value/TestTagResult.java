package com.alibaba.hitsdb.client.value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.hitsdb.client.value.response.TagResult;

public class TestTagResult {

    @Test
    public void testJSONToResult() {
        String json = "[{\"host\":\"127.0.0.1\"},{\"host\":\"127.0.0.1\"},{\"host\":\"127.0.0.1.012\"}]";

        try {
            JSONArray array = JSON.parseArray(json);
            // System.out.println(array);
            Assert.assertEquals(array.toString(),
                    "[{\"host\":\"127.0.0.1\"},{\"host\":\"127.0.0.1\"},{\"host\":\"127.0.0.1.012\"}]");
            List<TagResult> arrayList = new ArrayList<TagResult>(array.size());

            Iterator<Object> iterator = array.iterator();
            while (iterator.hasNext()) {
                JSONObject object = (JSONObject) iterator.next();
                Set<Entry<String, Object>> entrySet = object.entrySet();
                for (Entry<String, Object> entry : entrySet) {
                    String key = entry.getKey();
                    Assert.assertNotEquals(key, null);
                    Object valueObject = entry.getValue();
                    Assert.assertNotEquals(key, null);
                    String value = valueObject.toString();
                    TagResult tagResult = new TagResult(key, value);
                    arrayList.add(tagResult);
                    break;
                }
            }
            Assert.assertEquals(arrayList.size(), 3);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

}
