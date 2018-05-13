package com.aliyun.hitsdb.client.value.response;

import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.value.JSONValue;

public class LookupDetailedResult extends JSONValue {
    private Map<String, String> tags = new LinkedHashMap<String, String>();
    private String metric;
    private String tsuid;

    public LookupDetailedResult() {
        super();
    }

    public String getTsuid() {
        return tsuid;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTsuid(String tsuid) {
        this.tsuid = tsuid;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect);
    }
}
