package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.util.Objects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiFieldSubQuery {

    private String metric;
    private Map<String, String> tags;
    private List<Filter> filters;
    private List<MultiFieldSubQueryDetails> fields;
    private Integer limit;
    private Integer offset;
    private int index;

    public static class Builder {
        private String metric;
        private Integer limit;
        private Integer offset;
        private Map<String, String> tags = new HashMap<String, String>();
        private List<Filter> filters = new ArrayList<Filter>();
        private List<MultiFieldSubQueryDetails> fieldsInfo = new ArrayList<MultiFieldSubQueryDetails>();

        public Builder(final String metric) {
            Objects.requireNonNull(metric, "metric name");
            if (metric.isEmpty()) {
                throw new IllegalArgumentException("metric tag name cannot be empty.");
            }
            this.metric = metric;
        }

        public Builder limit() {
            this.limit = 0;
            return this;
        }

        public Builder offset() {
            this.offset = 0;
            return this;
        }

        public Builder limit(Integer limit) {
            if (limit != null) {
                if (limit < 0 || limit > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Illegal limit value.");
                }
                this.limit = limit;
            }
            return this;
        }

        public Builder offset(Integer offset) {
            if (offset != null) {
                if (offset < 0 || offset > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Illegal offset value.");
                }
                this.offset = offset;
            }
            return this;
        }

        public Builder fieldsInfo(List<MultiFieldSubQueryDetails> fieldsInfo) {
            if (fieldsInfo != null) {
                this.fieldsInfo.addAll(fieldsInfo);
            }
            return this;
        }

        public Builder fieldsInfo(MultiFieldSubQueryDetails fieldInfo) {
            if (fieldInfo != null) {
                this.fieldsInfo.add(fieldInfo);
            }
            return this;
        }

        /**
         * add a tagkey and tagvalue
         * when tagk or tagv is null, we simply ignore this given tag filter as it's not valid.
         * @param tagk tagkey
         * @param tagv tagvalue
         * @return Builder
         */
        public Builder tag(String tagk, String tagv) {
            if (filters != null && !filters.isEmpty()) {
                throw new IllegalArgumentException("Tags and filters cannot co-exist in the query.");
            }
            if (tagk != null && tagv != null && !tagk.isEmpty() && !tagv.isEmpty()) {
                this.tags.put(tagk, tagv);
            }
            return this;
        }

        /**
         * add the tags
         * @param tags the map
         * @return Builder
         */
        public Builder tags(Map<String, String> tags) {
            if (filters != null && !filters.isEmpty()) {
                throw new IllegalArgumentException("Tags and filters cannot co-exist in the query.");
            }
            if (tags != null) {
                this.tags.putAll(tags);
            }
            return this;
        }

        public Builder filter(Filter filter) {
            if (tags != null && !tags.isEmpty()) {
                throw new IllegalArgumentException("filters and tags cannot co-exist in the query.");
            }
            filters.add(filter);
            return this;
        }

        public Builder filters(List<Filter> filters) {
            if (tags != null && !tags.isEmpty()) {
                throw new IllegalArgumentException("filters and tags cannot co-exist in the query.");
            }
            filters.addAll(filters);
            return this;
        }

        public MultiFieldSubQuery build() {
            MultiFieldSubQuery subQuery = new MultiFieldSubQuery();
            subQuery.metric = this.metric;
            if (this.tags != null && !this.tags.isEmpty()) {
                subQuery.tags = this.tags;
            } else {
                subQuery.tags = null;
            }
            if (this.filters != null && !filters.isEmpty()) {
                subQuery.filters = this.filters;
            } else {
                subQuery.filters = null;
            }

            if (this.fieldsInfo == null || this.fieldsInfo.isEmpty()) {
                throw new IllegalArgumentException("Missing field info.");
            }
            subQuery.fields = this.fieldsInfo;

            if (this.limit != null && this.limit > 0) {
                subQuery.limit = this.limit;
            }

            if (this.offset != null && this.offset > 0) {
                subQuery.offset = this.offset;
            }

            return subQuery;
        }
    }

    /**
     * Multi-valued sub query builder
     * @param name metric tag name
     * @return
     */
    public static Builder metric(String name) {
        Builder builder = new Builder(name);
        return builder;
    }

    public String getMetric() {
        return metric;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return this.index;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public String toJSON() {
        return JSON.toJSONString(this);
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getOffset() {
        return offset;
    }

    public List<MultiFieldSubQueryDetails> getFields() {
        return fields;
    }
}
