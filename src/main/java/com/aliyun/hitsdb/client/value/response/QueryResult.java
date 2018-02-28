package com.aliyun.hitsdb.client.value.response;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.hitsdb.client.value.JSONValue;

public class QueryResult extends JSONValue {
    private String metric;
    private Map<String, String> tags;
    private List<String> aggregateTags;
    private LinkedHashMap<Long, Number> dps = new LinkedHashMap<Long, Number>();

    public List<String> getAggregateTags() {
        return aggregateTags;
    }

    public void setAggregateTags(List<String> aggregateTags) {
        this.aggregateTags = aggregateTags;
    }

    public LinkedHashMap<Long, Number> getDps() {
        return dps;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setDps(LinkedHashMap<Long, Number> dps) {
        this.dps = dps;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

}
