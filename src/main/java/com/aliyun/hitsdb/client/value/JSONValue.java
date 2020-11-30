package com.aliyun.hitsdb.client.value;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class JSONValue {

    public static <T extends JSONValue> T parseObject(String json, Class<T> clazz) {
        T object = JSON.parseObject(json, clazz);
        return object;
    }

    /*
    public static <T> List<T> parseList(String json, Class<T> clazz) {
        List<T> objectList = JSON.parseObject(json, new TypeReference<List<T>>() {});
        return objectList;
    }
    */

    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.SortField, SerializerFeature.SortField.MapSortField);
    }

    @Override
    public String toString() {
        return toJSON();
    }

    public void appendJSON(final StringBuilder sb) {
        sb.append(toJSON());
    }

}
