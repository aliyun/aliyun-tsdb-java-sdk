package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.util.Objects;
import com.aliyun.hitsdb.client.value.type.Aggregator;

public class MultiFieldSubQueryDetails {
    private String field;
    private String alias;
    private String aggregator;
    private Aggregator aggregatorType;
    private String downsample;
    private Boolean rate;
    private Integer top;
    private String dpValue;
    private String preDpValue;

    public static class Builder {
        private String field;
        private String alias = null;
        private Aggregator aggregatorType;
        private String downsample = null;
        private Boolean rate = false;
        private Integer top = 0;
        private String dpValue = null;
        private String preDpValue = null;

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
         * Set field alias name in result
         * @param alias
         * @return
         */
        public Builder alias(String alias) {
            if (alias != null && !alias.isEmpty()) {
                this.alias = alias;
            }
            return this;
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

        /**
         * Set data point value filter
         * @param dpValue
         * @return
         */
        public Builder dpValue(String dpValue) {
            if (dpValue != null && !dpValue.isEmpty() && validateDpValue(dpValue)) {
                this.dpValue = dpValue;
            }
            return this;
        }

        /**
         * Set data point pre-value filter
         * @param preDpValue
         * @return
         */
        public Builder preDpValue(String preDpValue) {
            if (preDpValue != null && !preDpValue.isEmpty() && validateDpValue(preDpValue)) {
                this.preDpValue = preDpValue;
            }
            return this;
        }

        /**
         * Set top operation
         * @param top
         * @return
         */
        public Builder top(Integer top) {
            if (top < 0 || top > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Illegal top operator value.");
            }
            this.top = top;
            return this;
        }

        /**
         * Validate dpValue input
         * @param dpValue
         * @return
         */
        private Boolean validateDpValue(final String dpValue) {
            // Query value filtering field's length is minimum 2.
            if (dpValue.length() == 1) {
                return false;
            }

            int index = 0;
            char[] value_array = dpValue.toCharArray();
            for (index = 0; index < dpValue.length(); index++) {
                // Find boundary between comparison operator and value.
                if (value_array[index] != '<'
                        && value_array[index] != '>'
                        && value_array[index] != '!'
                        && value_array[index] != '=') {
                    break;
                }
            }

            // field value does not contain valid comparison operator.
            if (index == 0) {
                return false;
            }

            return true;
        }

        public MultiFieldSubQueryDetails build() {
            MultiFieldSubQueryDetails fieldDetails = new MultiFieldSubQueryDetails();
            fieldDetails.field = this.field;
            if (this.alias != null && !this.alias.isEmpty()) {
                fieldDetails.alias = this.alias;
            }
            fieldDetails.aggregatorType = this.aggregatorType;
            fieldDetails.aggregator = this.aggregatorType.getName();
            fieldDetails.downsample = this.downsample;
            fieldDetails.rate = this.rate;
            if (this.dpValue != null) {
                fieldDetails.dpValue = this.dpValue;
            }
            if (this.preDpValue != null) {
                fieldDetails.preDpValue = this.preDpValue;
            }
            fieldDetails.top = this.top;
            return fieldDetails;
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

    public String getPreDpValue() {
        return preDpValue;
    }

    public String getDownsample() {
        return downsample;
    }

    public Boolean getRate() {
        return rate;
    }

    public Integer getTop() {
        return top;
    }

    public String getAlias() {
        return alias;
    }
}
