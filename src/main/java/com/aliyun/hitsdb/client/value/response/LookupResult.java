package com.aliyun.hitsdb.client.value.response;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.value.JSONValue;
import com.aliyun.hitsdb.client.value.request.LookupTagFilter;

public class LookupResult extends JSONValue {
    private String type;
    private String metric;
    private List<LookupTagFilter> tags = new ArrayList<LookupTagFilter>();
    private Integer limit = null;
    private int time;
    private List<LookupDetailedResult> results = new ArrayList<LookupDetailedResult>();
    private int totalResults;

    public LookupResult() {
        super();
    }

    public String getType() {
        return type;
    }

    public String getMetric() {
        return metric;
    }

    public List<LookupTagFilter> getTags() {
        return tags;
    }

    public Integer getLimit() {
        return limit;
    }

    public int getTime() {
        return time;
    }

    public List<LookupDetailedResult> getResults() {
        return results;
    }

    public int getTotalResults() {
        return totalResults;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(List<LookupTagFilter> tags) {
        this.tags = tags;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public void setResults(List<LookupDetailedResult> results) {
        this.results = results;
    }

    public void setTotalResults(int totalResults) {
        this.totalResults = totalResults;
    }

    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect);
    }
}
