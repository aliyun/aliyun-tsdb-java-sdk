package com.aliyun.hitsdb.client.value;

import java.util.*;
import java.util.Map.Entry;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.value.response.TagResult;

public class TestTagResult {
    private static TimeZone defaultTz;

    @BeforeClass
    public static void setup() {
        defaultTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8:00"));
    }

    @AfterClass
    public static void finish() {
        // reset
        TimeZone.setDefault(defaultTz);
    }

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
