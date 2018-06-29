package com.aliyun.hitsdb.client.value.response;

import java.util.*;

import com.aliyun.hitsdb.client.value.JSONValue;

public class QueryResult extends JSONValue {
    private String metric;
    private Map<String, String> tags;
    private List<String> aggregateTags;
    private LinkedHashMap<Long, Object> dps = new LinkedHashMap<Long, Object>();
    private LinkedHashMap<Long, String> sdps = new LinkedHashMap<Long, String>();

    public List<String> getAggregateTags() {
        return aggregateTags;
    }

    public void setAggregateTags(List<String> aggregateTags) {
        this.aggregateTags = aggregateTags;
    }

    public LinkedHashMap<Long, Object> getDps() {
        return dps;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setDps(LinkedHashMap<Long, Object> dps) {
        this.dps = dps;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public LinkedHashMap<Long, String> getSdps() {
        return sdps;
    }

    public void setSdps(LinkedHashMap<Long, String> sdps) {
        this.sdps = sdps;
    }

    public String tagsToString() {
        if (tags == null || tags.isEmpty()) {
            return null;
        }
        Set<String> tagks = new TreeSet<String>();
        tagks.addAll(tags.keySet());
        StringBuilder tagsString = new StringBuilder();
        boolean firstTag = true;
        for (String tagk : tagks) {
            if (firstTag) {
                tagsString.append(tagk).append("$").append(tags.get(tagk));
                firstTag = false;
            } else {
                tagsString.append("$").append(tagk).append("$").append(tags.get(tagk));
            }
        }
        return tagsString.toString();
    }
}
