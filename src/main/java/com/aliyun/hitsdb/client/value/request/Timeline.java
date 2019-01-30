package com.aliyun.hitsdb.client.value.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.hitsdb.client.value.JSONValue;

public class Timeline extends JSONValue {

    public static class Builder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();
        /**
         * Optional fields input parameter.
         * Used for fields' timelines under specific metric.
         */
        private List<String> fields = null;

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

        public Builder fields(List<String> fields) {
            if (fields == null || fields.isEmpty()) {
                return this;
            }
            this.fields = new ArrayList<String>();
            this.fields.addAll(fields);
            return this;
        }

        public Timeline build() {
            Timeline timeline = new Timeline();
            timeline.metric = this.metric;
            timeline.tags = this.tags;
            if (this.fields != null && !this.fields.isEmpty()) {
                timeline.fields = fields;
            }
            return timeline;
        }
    }

    public static Builder metric(String metric) {
        return new Builder(metric);
    }

    private String metric;
    private Map<String, String> tags;
    /**
     * Optional fields input parameter.
     * Used for fields' timelines under specific metric.
     */
    private List<String> fields = null;

    public Timeline() {
        super();
    }

    public List<String> getFields() {
        return fields;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }
}
