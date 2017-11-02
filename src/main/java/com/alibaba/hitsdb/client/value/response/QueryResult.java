package com.alibaba.hitsdb.client.value.response;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.hitsdb.client.value.JSONValue;

public class QueryResult extends JSONValue {
    private String metric;
    private Map<String, String> tags;
    private List<String> aggregateTags;
    private LinkedHashMap<Integer, Number> dps = new LinkedHashMap<Integer, Number>();
    private LinkedHashMap<Integer, String> sdps = new LinkedHashMap<Integer, String>();

    public List<String> getAggregateTags() {
        return aggregateTags;
    }

    public void setAggregateTags(List<String> aggregateTags) {
        this.aggregateTags = aggregateTags;
    }

    public LinkedHashMap<Integer, Number> getDps() {
        return dps;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setDps(LinkedHashMap<Integer, Number> dps) {
        this.dps = dps;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public LinkedHashMap<Integer, String> getSdps() {
        return sdps;
    }

    public void setSdps(LinkedHashMap<Integer, String> sdps) {
        this.sdps = sdps;
    }

}
