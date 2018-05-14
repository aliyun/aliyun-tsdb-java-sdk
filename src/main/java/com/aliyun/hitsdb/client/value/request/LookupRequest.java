package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.ArrayList;
import java.util.List;

public class LookupRequest extends JSONValue {

    public static class Builder {
        private String metric;
        private List<LookupTagFilter> tags = new ArrayList<LookupTagFilter>();
        private Integer limit = null;

        public Builder() {
            super();
        }

        public Builder(String metric) {
            this.metric = metric;
        }

        public Builder(String key, String value) {
            LookupTagFilter lookupTagFilter = new LookupTagFilter(key, value);
            tags.clear();
            this.tags.add(lookupTagFilter);
        }

        public Builder(List<LookupTagFilter> tags) {
            this.tags = tags;
        }

        public Builder(LookupTagFilter lookupTagFilter) {
            tags.clear();
            this.tags.add(lookupTagFilter);
        }

        public LookupRequest.Builder metric(String metric) {
            this.metric = metric;
            return this;
        }

        public LookupRequest.Builder tags(String key, String value) {
            LookupTagFilter lookupTagFilter = new LookupTagFilter(key, value);
            this.tags.add(lookupTagFilter);
            return this;
        }

        public LookupRequest.Builder tags(List<LookupTagFilter> tags) {
            this.tags.addAll(tags);
            return this;
        }

        public LookupRequest.Builder tags(LookupTagFilter lookupTagFilter) {
            this.tags.add(lookupTagFilter);
            return this;
        }

        public LookupRequest.Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public LookupRequest build() {
            LookupRequest lookupRequest = new LookupRequest();
            lookupRequest.metric = this.metric;
            lookupRequest.tags = this.tags;
            lookupRequest.limit = this.limit;
            return lookupRequest;
        }
    }

    private String metric;
    private List<LookupTagFilter> tags = new ArrayList<LookupTagFilter>();
    private Integer limit = null;

    public static LookupRequest.Builder metric(String metric) {
        return new LookupRequest.Builder(metric);
    }

    public static LookupRequest.Builder tags(String key, String value) {
        return new LookupRequest.Builder(key, value);
    }

    public static LookupRequest.Builder tags(LookupTagFilter lookupTagFilter) {
        return new LookupRequest.Builder(lookupTagFilter);
    }

    public static LookupRequest.Builder tags(List<LookupTagFilter> lookupTagFilters) {
        return new LookupRequest.Builder(lookupTagFilters);
    }

    public LookupRequest() {
        super();
    }

    public LookupRequest(String metric, List<LookupTagFilter> tags, int limit) {
        super();
        this.metric = metric;
        this.tags = tags;
        this.limit = limit;
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

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(List<LookupTagFilter> tags) {
        this.tags = tags;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect);
    }
}
