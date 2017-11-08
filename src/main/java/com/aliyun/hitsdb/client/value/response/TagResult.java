package com.aliyun.hitsdb.client.value.response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.value.JSONValue;

public class TagResult extends JSONValue {
    private String tagKey;
    private String tagValue;

    public TagResult(String tagKey, String tagValue) {
        this.tagKey = tagKey;
        this.tagValue = tagValue;
    }

    public String getTagKey() {
        return tagKey;
    }

    public String getTagValue() {
        return tagValue;
    }

    public static List<TagResult> parseList(String json) {
        JSONArray array = JSON.parseArray(json);
        List<TagResult> arrayList = new ArrayList<TagResult>(array.size());

        Iterator<Object> iterator = array.iterator();
        while (iterator.hasNext()) {
            JSONObject object = (JSONObject) iterator.next();
            Set<Entry<String, Object>> entrySet = object.entrySet();
            for (Entry<String, Object> entry : entrySet) {
                String key = entry.getKey();
                String value = entry.getValue().toString();
                TagResult tagResult = new TagResult(key, value);
                arrayList.add(tagResult);
                break;
            }
        }

        return arrayList;
    }

    @Override
    public String toJSON() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(tagKey, tagValue);
        return jsonObject.toJSONString();
    }

}