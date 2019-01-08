package com.aliyun.hitsdb.client.value.request;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.util.Objects;
import com.aliyun.hitsdb.client.value.JSONValue;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MultiFieldPoint extends JSONValue {
    public static class MetricBuilder {
        private String metric;
        private Map<String, String> tags = new HashMap<String, String>();
        private Map<String, Object> fields = new HashMap<String, Object>();
        private Long timestamp;

        public MetricBuilder(final String metric) {
            Objects.requireNonNull(metric, "metric name");
            if (metric.isEmpty()) {
                throw new IllegalArgumentException("Metric name cannot be empty.");
            }
            this.metric = metric;
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
        public MultiFieldPoint build() {
            return build(true);
        }

        public MultiFieldPoint build(boolean checkPoint) {
            MultiFieldPoint multiFieldPoint = new MultiFieldPoint();
            multiFieldPoint.metric = this.metric;
            multiFieldPoint.tags = this.tags;
            multiFieldPoint.fields = this.fields;
            multiFieldPoint.timestamp = this.timestamp;
            if (HiTSDBConfig.Builder.ProducerThreadSerializeSwitch) {
                multiFieldPoint.json = buildJSON(multiFieldPoint);
            }

            if (checkPoint) {
                MultiFieldPoint.checkPoint(multiFieldPoint);
            }

            return multiFieldPoint;
        }

        /**
         * convert to json
         * @param point
         * @return String
         */
        private String buildJSON(MultiFieldPoint point) {
            return JSON.toJSONString(point);
        }
    }

    /**
     * set the metric
     * @param name metric tag key
     * @return MetricBuilder get a builder
     */
    public static MetricBuilder metric(String name) {
        return new MetricBuilder(name);
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
     * @param multiFieldPoint multi-valued data point
     */
    public static void checkPoint(MultiFieldPoint multiFieldPoint) {
        if (multiFieldPoint.metric == null || multiFieldPoint.metric.length() == 0) {
            throw new IllegalArgumentException("Metric can't be empty");
        } else {
            for (int i = 0; i < multiFieldPoint.metric.length(); i++) {
                final char c = multiFieldPoint.metric.charAt(i);
                if (!checkChar(c)) {
                    throw new IllegalArgumentException("There is an invalid character in metric. The char is '" + c + "'");
                }
            }
        }

        if (multiFieldPoint.timestamp == null) {
            throw new IllegalArgumentException("Timestamp can't be null");
        }

        if (multiFieldPoint.timestamp <= 0) {
            throw new IllegalArgumentException("Timestamp can't be less than or equal to 0");
        }

        if (multiFieldPoint.fields == null || multiFieldPoint.fields.isEmpty()) {
            throw new IllegalArgumentException("Fields can't be null or empty");
        }

        for (Map.Entry<String, Object> field : multiFieldPoint.fields.entrySet()) {
            if (field.getKey() == null || field.getKey().isEmpty()) {
                throw new IllegalArgumentException("Field name can't be null or empty.");
            } else {
                for (int i = 0; i < field.getKey().length(); i++) {
                    final char c = field.getKey().charAt(i);
                    if (!checkChar(c)) {
                        throw new IllegalArgumentException("There is an invalid character in field. The char is '" + c + "'");
                    }
                }
            }

            if (field.getValue() == null) {
                throw new IllegalArgumentException("Field value can't be null or empty.");
            }

            if (field.getValue() instanceof String && ((String) field.getValue()).isEmpty()) {
                throw new IllegalArgumentException("String field value can't be empty");
            }

            if (field.getValue() instanceof Number && field.getValue() == (Number) Double.NaN) {
                throw new IllegalArgumentException("Number field value can't be NaN");
            }

            if (field.getValue() instanceof Number && field.getValue() == (Number) Double.POSITIVE_INFINITY) {
                throw new IllegalArgumentException("Number field value can't be POSITIVE_INFINITY");
            }

            if (field.getValue() instanceof Number && field.getValue() == (Number) Double.NEGATIVE_INFINITY) {
                throw new IllegalArgumentException("Number field value can't be NEGATIVE_INFINITY");
            }
        }

        if (multiFieldPoint.tags == null || multiFieldPoint.tags.size() == 0) {
            throw new IllegalArgumentException("At least one tag is needed");
        }

        // Measurement metric is automatically inserted into tags map. No need to separately check metric.
        for (Map.Entry<String, String> entry : multiFieldPoint.tags.entrySet()) {
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
