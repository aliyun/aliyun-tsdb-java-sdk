package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.value.JSONValue;

public class LookupTagFilter extends JSONValue {
    private String key;
    private String value;

    public LookupTagFilter() {
        super();
    }

    public LookupTagFilter(String key, String value) {
        this.value = value;
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return  value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect);
    }
}