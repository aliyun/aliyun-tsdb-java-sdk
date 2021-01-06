package com.aliyun.hitsdb.client.value.request;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONType;
import com.aliyun.hitsdb.client.util.Objects;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import com.aliyun.hitsdb.client.value.type.FilterType;
import com.aliyun.hitsdb.client.value.type.Granularity;

@JSONType(ignores = {"aggregatorType", "granularityType"})
public class SubQuery {
    private int index;
    private String aggregator;
    private Aggregator aggregatorType;
    private String metric;
    private String downsample;
    private Boolean rate;
    private RateOptions rateOptions;
    /**
     * the delta field of SubQuery is supported at the Server side from v.2.5.9
     * @since v0.2.5
     */
    private Boolean delta;
    private DeltaOptions deltaOptions;
    
    private Map<String, String> tags;
    private Granularity granularityType;
    private String granularity;
    private Boolean explicitTags;
    private Integer realTimeSeconds;
    private Integer limit;
    private Integer globalLimit;
    private Integer offset;
    private String dpValue;
    private String preDpValue;
    private List<Filter> filters;
    private Map<String, Map<String, Integer>> hint;

    public static class Builder {
        private Aggregator aggregatorType;
        private String metric;
        private String downsample;
        private Boolean rate;
        private RateOptions rateOptions;
        private Boolean delta;
        private DeltaOptions deltaOptions;
        private Integer limit;
        private Integer globalLimit;
        private Integer offset;
        private String dpValue;
        private String preDpValue;
        private Map<String, String> tags = new HashMap<String, String>();
        private Granularity granularityType;
        private Boolean explicitTags;
        private Integer realTimeSeconds;
        private List<Filter> filters;
        private Map<String, Map<String, Integer>> hint;

        public Builder(String metric, Aggregator aggregator) {
            Objects.requireNonNull(metric, "metric");
            Objects.requireNonNull(aggregator, "aggregator");
            this.metric = metric;
            this.aggregatorType = aggregator;
        }

        public Builder rate() {
            this.rate = true;
            return this;
        }

        public Builder rate(RateOptions rateOptions) {
            this.rate = true;
            this.rateOptions = rateOptions;
            return this;
        }

        public Builder delta() {
            this.delta = true;
            return this;
        }

        public Builder delta(DeltaOptions deltaOptions) {
            this.delta = true;
            this.deltaOptions = deltaOptions;
            return this;
        }

        @Deprecated
        public Builder limit() {
            this.limit = 0;
            return this;
        }

        @Deprecated
        public Builder offset() {
            this.offset = 0;
            return this;
        }

        @Deprecated
        public Builder dpValue() {
            this.dpValue = null;
            return this;
        }

        @Deprecated
        public Builder preDpValue() {
            this.preDpValue = null;
            return this;
        }

        public Builder granularity(Granularity granularity) {
            if (granularity == null) {
                return this;
            }

            if (granularity.equals(Granularity.S1)) {
                return this;
            }

            this.granularityType = granularity;
            return this;
        }

        /**
         * add a filter
         *
         * @param type    FilterType
         * @param tagk    tagkey
         * @param filter  filter
         * @param groupBy grouyBy
         * @return Builder
         */
        public Builder filter(FilterType type, String tagk, String filter, boolean groupBy) {
            if (filters == null) {
                filters = new ArrayList<Filter>();
            }

            Filter f = new Filter();
            f.setType(type);
            f.setFilter(filter);
            f.setTagk(tagk);
            if (groupBy) {
                f.setGroupBy(true);
            }

            filters.add(f);
            return this;
        }

        /**
         * add a filter
         *
         * @param filter filter
         * @return Builder
         */
        public Builder filter(Filter filter) {
            if (filters == null) {
                filters = new ArrayList<Filter>();
            }

            filters.add(filter);
            return this;
        }

        /**
         * add a tag value filter
         *
         * @param type   tupe
         * @param tagk   tagkey
         * @param filter filter
         * @return Builder
         */
        public Builder filter(FilterType type, String tagk, String filter) {
            if (type == null || tagk == null || tagk.isEmpty() || filter == null || filter.isEmpty()) {
                return this;
            }
            this.filter(type, tagk, filter, false);
            return this;
        }

        /**
         * Add a geo filter
         *
         * @param type   geo filter type
         * @param filter filter info
         * @return
         */
        public Builder filter(FilterType type, String filter) {
            // Geo filter sanity check
            if (type != FilterType.GeoBBox && type != FilterType.GeoDistance) {
                return this;
            }

            if (filters == null) {
                filters = new ArrayList<Filter>();
            }

            Filter f = new Filter();
            f.setType(type);
            f.setFilter(filter);

            filters.add(f);
            return this;
        }

        /**
         * set the rate
         *
         * @param rate rate
         * @return Builder
         */
        public Builder rate(Boolean rate) {
            if (rate != null) {
                this.rate = rate;
            }
            return this;
        }

        public Builder delta(Boolean delta) {
            if (delta != null) {
                this.delta = delta;
            }
            return this;
        }

        public Builder limit(Integer limit) {
            if (limit != null) {
                this.limit = limit;
            }
            return this;
        }

        public Builder globalLimit(Integer globalLimit) {
            if (limit != null) {
                this.globalLimit = globalLimit;
            }
            return this;
        }

        public Builder offset(Integer offset) {
            if (offset != null) {
                this.offset = offset;
            }
            return this;
        }

        public Builder dpValue(String dpValue) {
            if (dpValue != null && !dpValue.isEmpty()) {
                this.dpValue = dpValue;
            }
            return this;
        }

        public Builder preDpValue(String preDpValue) {
            if (preDpValue != null && !preDpValue.isEmpty()) {
                this.preDpValue = preDpValue;
            }
            return this;
        }

        /**
         * set the downsample
         *
         * @param downsample downsample
         * @return Builder
         */
        public Builder downsample(String downsample) {
            if (downsample != null && !downsample.isEmpty()) {
                this.downsample = downsample;
            }
            return this;
        }

        /**
         * add a tagkey and tagvalue
         *
         * @param tagk tagkey
         * @param tagv tagvalue
         * @return Builder
         */
        public Builder tag(String tagk, String tagv) {
            if (tagk == null || tagk.isEmpty() || tagv == null || tagv.isEmpty()) {
                return this;
            }
            this.tags.put(tagk, tagv);
            return this;
        }

        /**
         * add the tags
         *
         * @param tags the map
         * @return Builder
         */
        public Builder tag(Map<String, String> tags) {
            if (tags != null) {
                this.tags.putAll(tags);
            }
            return this;
        }

        public Builder explicitTags() {
            this.explicitTags = true;
            return this;
        }

        public Builder explicitTags(Boolean explicitTags) {
            if (explicitTags != null) {
                this.explicitTags = explicitTags;
            }
            return this;
        }

        public Builder realtime(Integer secondes) {
            if (secondes != null) {
                this.realTimeSeconds = secondes;
            }
            return this;
        }

        public Builder realtime(Long time, TimeUnit unit) {
            if (time != null && unit != null) {
                this.realTimeSeconds = (int) unit.toSeconds(time);
            }
            return this;
        }

        public Builder hint(Map<String, Map<String, Integer>> hint) {
            this.hint = hint;
            return this;
        }

        public SubQuery build() {
            SubQuery subQuery = new SubQuery();
            subQuery.aggregatorType = this.aggregatorType;
            subQuery.aggregator = this.aggregatorType.getName();
            subQuery.downsample = this.downsample;
            subQuery.metric = this.metric;
            subQuery.tags = this.tags;

            if ((this.delta != null) && (this.delta == true) && (this.rate != null) && (this.rate == true)) {
                throw new IllegalArgumentException("\"delta\" and \"rate\" cannot be specified at the same time");
            }
            subQuery.rate = this.rate;
            if (this.rateOptions != null) {
				subQuery.rateOptions = this.rateOptions;
            }
            subQuery.delta = this.delta;
            if (this.deltaOptions != null) {
                subQuery.deltaOptions = this.deltaOptions;
            }
            subQuery.granularityType = this.granularityType;
            subQuery.realTimeSeconds = this.realTimeSeconds;
            subQuery.explicitTags = this.explicitTags;

            if (this.limit != null && this.limit > 0) {
                subQuery.limit = this.limit;
            }

            if (this.globalLimit != null && this.globalLimit > 0) {
                subQuery.globalLimit = this.globalLimit;
            }

            if (this.offset != null && this.offset > 0) {
                subQuery.offset = this.offset;
            }

            if (this.dpValue != null) {
                subQuery.dpValue = this.dpValue;
            }

            if (this.preDpValue != null) {
                subQuery.preDpValue = this.preDpValue;
            }

            if (this.granularityType != null) {
                subQuery.granularity = this.granularityType.getName();
            }
            subQuery.filters = this.filters;
            if (subQuery.tags != null && subQuery.tags.isEmpty()) {
                subQuery.tags = null;
            }
            subQuery.hint = this.hint;
            return subQuery;
        }
    }

    public static class MetricBuilder {
        private String metric;

        public MetricBuilder(String metric) {
            this.metric = metric;
        }

        public Builder aggregator(Aggregator aggregator) {
            return new Builder(metric, aggregator);
        }
    }

    public static class AggregatorBuilder {
        private Aggregator aggregator;

        public AggregatorBuilder(Aggregator aggregator) {
            this.aggregator = aggregator;
        }

        public Builder metric(String metric) {
            return new Builder(metric, aggregator);
        }
    }

    public static Builder metric(String metric, Aggregator aggregator) {
        Builder builder = new Builder(metric, aggregator);
        return builder;
    }

    public static MetricBuilder metric(String metric) {
        return new MetricBuilder(metric);
    }

    public static AggregatorBuilder aggregator(Aggregator aggregator) {
        return new AggregatorBuilder(aggregator);
    }

    public Aggregator getAggregatorType() {
        return aggregatorType;
    }

    public String getAggregator() {
        return aggregator;
    }

    public String getMetric() {
        return metric;
    }

    public String getDownsample() {
        return downsample;
    }

    public Boolean getRate() {
        return rate;
    }

    public RateOptions getRateOptions() {
        return rateOptions;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String toJSON() {
        return JSON.toJSONString(this);
    }

    public Granularity getGranularityType() {
        return granularityType;
    }

    public String getGranularity() {
        return granularity;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return this.index;
    }

    public Boolean isExplicitTags() {
        return explicitTags;
    }

    public Integer getRealTimeSeconds() {
        return realTimeSeconds;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getGlobalLimit() {
        return globalLimit;
    }

    public Integer getOffset() {
        return offset;
    }

    public String getDpValue() {
        return dpValue;
    }

    public String getPreDpValue() {
        return preDpValue;
    }

    public Boolean getDelta() {
        return delta;
    }

    public DeltaOptions getDeltaOptions() {
        return deltaOptions;
    }

    public Map<String, Map<String, Integer>> getHint() {
        return hint;
    }

    public void setHint(Map<String, Map<String, Integer>> hint) {
        this.hint = hint;
    }
}
