package com.aliyun.hitsdb.client.value.request;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.hitsdb.client.value.JSONValue;

public class Timeline extends JSONValue {

    public static class Builder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();

        public Builder(String metric) {
            this.metric = metric;
        }

        public Builder tag(String tagk, String tagv) {
            this.tags.put(tagk, tagv);
            return this;
        }

        public Builder tag(Map<String, String> tags) {
            this.tags.putAll(tags);
            return this;
        }

        public Timeline build() {
            Timeline timeline = new Timeline();
            timeline.metric = this.metric;
            timeline.tags = this.tags;
            return timeline;
        }

    }

    public static Builder metric(String metric) {
        return new Builder(metric);
    }

    private String metric;
    private Map<String, String> tags;

    public Timeline() {
        super();
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

}
