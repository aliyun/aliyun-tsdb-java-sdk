package com.aliyun.hitsdb.client.value.request;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.util.Objects;
import com.aliyun.hitsdb.client.value.JSONValue;

@Deprecated
public class MultiValuedPoint extends JSONValue {
    public static class MetricBuilder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();
        private Map<String, Object> fields = new HashMap<String, Object>();
        private Long timestamp;

        public MetricBuilder(final String metricTagKey, final String metricTagValue) {
            Objects.requireNonNull(metricTagKey, "metric tag key");
            Objects.requireNonNull(metricTagValue, "metric tag value");
            if (metricTagKey.isEmpty()) {
                throw new IllegalArgumentException("Measurement tag key cannot be empty.");
            }
            if (metricTagValue.isEmpty()) {
                throw new IllegalArgumentException("Measurement tag value cannot be empty.");
            }

            tags.put(metricTagKey, metricTagValue);
            this.metric = metricTagValue;
        }

        /**
         * add a TagKey and TagValue
         * @param tagName tagName
         * @param value value
         * @return MetricBuilder
         */
        public MetricBuilder tag(final String tagName, final String value) {
            Objects.requireNonNull(tagName, "tag key");
            Objects.requireNonNull(value, "tag value");
            if (!tagName.isEmpty() && !value.isEmpty()) {
                tags.put(tagName, value);
            }
            return this;
        }

        /**
         * add the tags
         * @param tags a map
         * @return MetricBuilder
         */
        public MetricBuilder tag(final Map<String, String> tags) {
            if (tags != null) {
                this.tags.putAll(tags);
            }
            return this;
        }

        /**
         * set timestamp
         * @param timestamp time
         * @return MetricBuilder
         */
        public MetricBuilder timestamp(Long timestamp) {
            Objects.requireNonNull(timestamp, "timestamp");
            this.timestamp = timestamp;
            return this;
        }

        /**
         * set timestamp
         * @param date java.util.Date
         * @return MetricBuilder
         */
        public MetricBuilder timestamp(Date date) {
            Objects.requireNonNull(date, "date timestamp");
            this.timestamp = date.getTime();
            return this;
        }

        /**
         * Set fields (Multi valued structure)
         * For null value, we do not throw exception and we tolerate it by not adding to fields map.
         * @param fieldName
         * @param fieldValue
         * @return
         */
        public MetricBuilder fields(final String fieldName, final Object fieldValue) {
            Objects.requireNonNull(fieldName, "field name");
            if (fieldValue != null) {
                this.fields.put(fieldName, fieldValue);
            }
            return this;
        }

        public MetricBuilder fields(final Map<String, Object> fields) {
            if (fields != null) {
                this.fields.putAll(fields);
            }
            return this;
        }

        /**
         * build a multi-valued data point
         * @return Point
         */
        public MultiValuedPoint build() {
            return build(true);
        }

        public MultiValuedPoint build(boolean checkPoint) {
            MultiValuedPoint multiValuedPoint = new MultiValuedPoint();
            multiValuedPoint.metric = this.metric;
            multiValuedPoint.tags = this.tags;
            multiValuedPoint.fields = this.fields;
            multiValuedPoint.timestamp = this.timestamp;
            if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
                multiValuedPoint.json = buildJSON(multiValuedPoint);
            }

            if (checkPoint) {
                MultiValuedPoint.checkPoint(multiValuedPoint);
            }

            return multiValuedPoint;
        }

        /**
         * convert to json
         * @param point
         * @return String
         */
        private String buildJSON(MultiValuedPoint point) {
            return JSON.toJSONString(point);
        }
    }

    /**
     * set the metric
     * @param name metric tag key
     * @param value metric tag value
     * @return MetricBuilder get a builder
     */
    public static MetricBuilder metric(String name, String value) {
        return new MetricBuilder(name, value);
    }

    private String metric;
    private Map<String, String> tags;
    private Map<String, Object> fields;
    private Long timestamp;
    private String json;

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getFields() {
        return this.fields;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    @Override
    public String toJSON() {
        if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
            return this.json;
        } else {
            return super.toJSON();
        }
    }

    /**
     * If it is true, it is a legitimate character.
     * @param c char
     * @return
     */
    private static boolean checkChar(char c) {
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') ||
                c == '-' || c == '_' ||
                c == '.' || c == ' ' || c == ',' || c == '=' || c == '/' || c == ':' ||
                c == '(' || c == ')' || c == '[' || c == ']' || c == '\'' || c == '/' || c == '#' ||
                Character.isLetter(c);
    }

    /**
     * Checkout the point format
     * @param multiValuedPoint multi-valued data point
     */
    public static void checkPoint(MultiValuedPoint multiValuedPoint) {
        if (multiValuedPoint.metric == null || multiValuedPoint.metric.length() == 0) {
            throw new IllegalArgumentException("The measurement metric can't be empty");
        }

        if (multiValuedPoint.timestamp == null) {
            throw new IllegalArgumentException("The timestamp can't be null");
        }

        if (multiValuedPoint.timestamp <= 0) {
            throw new IllegalArgumentException("The timestamp can't be less than or equal to 0");
        }

        if (multiValuedPoint.fields == null || multiValuedPoint.fields.isEmpty()) {
            throw new IllegalArgumentException("The fields can't be null or empty");
        }

        for (Map.Entry<String, Object> field : multiValuedPoint.fields.entrySet()) {
            if (field.getKey() == null || field.getKey().isEmpty()) {
                throw new IllegalArgumentException("The field name can't be null or empty.");
            }

            if (field.getValue() == null) {
                throw new IllegalArgumentException("The field value can't be null or empty.");
            }

            if (field.getValue() instanceof String && ((String) field.getValue()).isEmpty()) {
                throw new IllegalArgumentException("The String field value can't be empty");
            }

            if (field.getValue() instanceof Number && field.getValue() == (Number) Double.NaN) {
                throw new IllegalArgumentException("The Number field value can't be NaN");
            }

            if (field.getValue() instanceof Number && field.getValue() == (Number) Double.POSITIVE_INFINITY) {
                throw new IllegalArgumentException("The Number field value can't be POSITIVE_INFINITY");
            }

            if (field.getValue() instanceof Number && field.getValue() == (Number) Double.NEGATIVE_INFINITY) {
                throw new IllegalArgumentException("The Number field value can't be NEGATIVE_INFINITY");
            }

            for (int i = 0; i < field.getKey().length(); i++) {
                final char c = field.getKey().charAt(i);
                if (!checkChar(c)) {
                    throw new IllegalArgumentException("There is an invalid character in metric. the char is '" + c + "'");
                }
            }
        }

        if (multiValuedPoint.tags == null || multiValuedPoint.tags.size() == 0) {
            throw new IllegalArgumentException("At least one tag is needed");
        }

        // Measurement metric is automatically inserted into tags map. No need to separately check metric.
        for (Entry<String, String> entry : multiValuedPoint.tags.entrySet()) {
            String tagkey = entry.getKey();
            String tagvalue = entry.getValue();

            for (int i = 0; i < tagkey.length(); i++) {
                final char c = tagkey.charAt(i);
                if (!checkChar(c)) {
                    throw new IllegalArgumentException("There is an invalid character in tagkey. the tagkey is + "
                            + tagkey + ", the char is '" + c + "'");
                }
            }

            for (int i = 0; i < tagvalue.length(); i++) {
                final char c = tagvalue.charAt(i);
                if (!checkChar(c)) {
                    throw new IllegalArgumentException("There is an invalid character in tagvalue. the tag is + <"
                            + tagkey + ":" + tagvalue + "> , the char is '" + c + "'");
                }
            }
        }
    }
}
