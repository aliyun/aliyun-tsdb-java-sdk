package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.value.type.Aggregator;

import java.util.Objects;

public class MultiValuedQueryMetricDetails {
    private String field;
    private String aggregator;
    private Aggregator aggregatorType;
    private String downsample;
    private Boolean rate;
    private String dpValue;

    public static class Builder {
        private String field;
        private Aggregator aggregatorType;
        private String downsample;
        private Boolean rate;
        private String dpValue;

        public Builder(String field, Aggregator aggregatorType) {
            Objects.requireNonNull(field, "field name");
            Objects.requireNonNull(aggregatorType, "aggregator");
            if (field.isEmpty()) {
                throw new IllegalArgumentException("Field name cannot be empty in multi-valued query.");
            }
            this.field = field;
            this.aggregatorType = aggregatorType;
        }

        /**
         * set the downsample
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
         * set the rate
         * @param rate rate
         * @return Builder
         */
        public Builder rate(Boolean rate) {
            if (rate != null) {
                this.rate = rate;
            }
            return this;
        }

        public Builder rate() {
            this.rate = true;
            return this;
        }

        public Builder dpValue() {
            this.dpValue = null;
            return this;
        }

        public Builder dpValue(String dpValue) {
            if (dpValue != null && !dpValue.isEmpty()) {
                this.dpValue = dpValue;
            }
            return this;
        }

        public MultiValuedQueryMetricDetails build() {
            MultiValuedQueryMetricDetails queryMetricDetails = new MultiValuedQueryMetricDetails();
            queryMetricDetails.aggregatorType = this.aggregatorType;
            queryMetricDetails.aggregator = this.aggregatorType.getName();
            queryMetricDetails.downsample = this.downsample;
            queryMetricDetails.field = this.field;
            queryMetricDetails.rate = this.rate;

            if (this.dpValue != null) {
                queryMetricDetails.dpValue = this.dpValue;
            }

            return queryMetricDetails;
        }
    }

    public static class FieldBuilder {
        private String field;

        public FieldBuilder(String field) {
            this.field = field;
        }

        public Builder aggregator(Aggregator aggregator) {
            return new Builder(field, aggregator);
        }
    }

    public static class AggregatorBuilder {
        private Aggregator aggregator;

        public AggregatorBuilder(Aggregator aggregator) {
            this.aggregator = aggregator;
        }

        public Builder field(String field) {
            return new Builder(field, aggregator);
        }
    }

    public static Builder field(String field, Aggregator aggregator) {
        Builder builder = new Builder(field, aggregator);
        return builder;
    }

    public static FieldBuilder field(String field) {
        return new FieldBuilder(field);
    }

    public static AggregatorBuilder aggregator(Aggregator aggregator) {
        return new AggregatorBuilder(aggregator);
    }

    public String getField() {
        return field;
    }

    public String getAggregator() {
        return aggregator;
    }

    public Aggregator getAggregatorType() {
        return aggregatorType;
    }

    public String getDpValue() {
        return dpValue;
    }

    public String getDownsample() {
        return downsample;
    }

    public Boolean getRate() {
        return rate;
    }
}
