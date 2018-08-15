package com.aliyun.hitsdb.client.value.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.aliyun.hitsdb.client.value.JSONValue;

public class MultiValuedQueryLastRequest extends JSONValue {
    
    private String metric;
    private Map<String, String> tags;
    private List<String> fields;

    public static class Builder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();
        private List<String> fields = new ArrayList<String>();

        public Builder(final String name, final String value) {
            // Measurement metric tag name and value are required for multivalued query last end point.
            Objects.requireNonNull(name, "metric tag name");
            Objects.requireNonNull(value, "metric tag value");
            if (name.isEmpty()) {
                throw new IllegalArgumentException("metric tag name cannot be empty for multi-valued query last.");
            }
            if (value.isEmpty()) {
                throw new IllegalArgumentException("metric tag value cannot be empty for multi-valued query last.");

            }
            tags.put(name, value);
            this.metric = value;
        }

        public Builder tag(String tagk, String tagv) {
            if (tagk != null && tagv != null && !tagk.isEmpty() && !tagv.isEmpty()) {
                this.tags.put(tagk, tagv);
            }
            return this;
        }

        public Builder tag(Map<String, String> tags) {
            if (tags == null) {
                this.tags.putAll(tags);
            }
            return this;
        }

        public Builder field(String field) {
            Objects.requireNonNull(field, "field name");
            if (field.isEmpty()) {
                throw new IllegalArgumentException("Field name cannot be empty for multi-valued query last.");
            }
            this.fields.add(field);
            return this;
        }

        public Builder field(List<String> fields) {
            Objects.requireNonNull(fields, "field names");
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("Field names cannot be empty for multi-valued query last.");
            }
            this.fields.addAll(fields);
            return this;
        }

        public MultiValuedQueryLastRequest build() {
            MultiValuedQueryLastRequest queryLastRequest = new MultiValuedQueryLastRequest();
            queryLastRequest.metric = this.metric;
            queryLastRequest.tags = this.tags;
            queryLastRequest.fields = this.fields;
            return queryLastRequest;
        }
    }

    /**
     * Multi-valued query last request builder
     * @param name metric tag name
     * @param value metric tag value
     * @return
     */
    public static Builder metric(String name, String value) {
        return new Builder(name, value);
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<String> getFields() {
        return fields;
    }
}
