package com.aliyun.hitsdb.client.value.request;

import java.util.*;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.util.Objects;

@Deprecated
public class MultiValuedSubQuery {
    private String metric;
    private Map<String, String> tags;
    private List<MultiValuedQueryMetricDetails> fieldsInfo;
    private Integer limit;
    private Integer offset;

    public static class Builder {
        private String metric;
        private Integer limit;
        private Integer offset;
        private Map<String, String> tags = new HashMap<String, String>();
        private List<MultiValuedQueryMetricDetails> fieldsInfo = new ArrayList<MultiValuedQueryMetricDetails>();

        public Builder(final String name, final String value) {
            Objects.requireNonNull(name, "metric tag name");
            Objects.requireNonNull(name, "metric tag value");
            if (name.isEmpty()) {
                throw new IllegalArgumentException("metric tag name cannot be empty.");
            }
            if (value.isEmpty()) {
                throw new IllegalArgumentException("metric tag value cannot be empty.");
            }
            tags.put(name, value);
            this.metric = value;
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
                this.limit = limit;
            }
            return this;
        }

        public Builder offset(Integer offset) {
            if (offset != null) {
                this.offset = offset;
            }
            return this;
        }

        public Builder fieldsInfo(List<MultiValuedQueryMetricDetails> fieldsInfo) {
            if (fieldsInfo != null) {
                this.fieldsInfo.addAll(fieldsInfo);
            }
            return this;
        }

        public Builder fieldsInfo(MultiValuedQueryMetricDetails fieldInfo) {
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
        public Builder tag(Map<String, String> tags) {
            if (tags != null) {
                this.tags.putAll(tags);
            }
            return this;
        }

        public MultiValuedSubQuery build() {
            MultiValuedSubQuery subQuery = new MultiValuedSubQuery();
            subQuery.metric = this.metric;
            subQuery.tags = this.tags;
            subQuery.fieldsInfo = this.fieldsInfo;

            if (this.limit != null) {
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
     * @param value metric tag value
     * @return
     */
    public static Builder metric(String name, String value) {
        Builder builder = new Builder(name, value);
        return builder;
    }

    public String getMetric() {
        return metric;
    }


    public Map<String, String> getTags() {
        return tags;
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

    public List<MultiValuedQueryMetricDetails> getFieldsInfo() {
        return fieldsInfo;
    }
}
